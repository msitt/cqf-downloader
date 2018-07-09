import email.utils as eut
import errno
import os
import queue
import re
import tempfile
import threading
import time
import traceback
import urllib.parse
from datetime import datetime

import requests
from bs4 import BeautifulSoup


class CqfDownloader:
    """Class to download CQF materials."""
    def __init__(self):
        self._session = requests.Session()
        custom_header = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:60.0) Gecko/20100101 Firefox/60.0',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
        }
        self._session.headers.update(custom_header)
        self._base_output_dir = ''
        self.verbose = False

        self.login_url = 'https://erp.fitchlearning.com/erp_live/2/index.php/PortalEx/6/0/login/'
        self.study_url = 'https://erp.fitchlearning.com/erp_live/2/index.php/PortalEx/6/0/page/25776/show/'

        self._IGNORED_7CITY_TYPES = set(['all', 'breeze', 'webex'])
        self._EXCLUDE_LIST = set([
            'http://en.literateprograms.org/High_Frequency_Finance'
        ])

        self._regex_7city_html_video = re.compile(r'^https?:\/\/.*(media\.fitchlearning\.com|fitchmediabucket|7citymedia).*\/CQF\/.*\.html(\?c=\d{10})?$', re.IGNORECASE)
        self._regex_ssrn = re.compile(r'^http:\/\/ssrn.com\/abstract=(\d+)$')
        self._regex_html = re.compile(r'^https?:\/\/.*\.html(\?.*)?$', re.IGNORECASE)
        self._regex_webex = re.compile(r'^https?:(\/\/\w*\.)?webex\.com\/.*\/playback\.php', re.IGNORECASE)
        self._regex_whitelist = re.compile(r'^https?:\/\/.*\.(mp4|avi|wmv|mov|flv|mp3|pdf|xls|xlsx|doc|docx|ppt|pptx|txt|zip|rar)(\?c=\d{10})?$', re.IGNORECASE)
        self._regex_mms = re.compile(r'^mms?:\/\/.*$', re.IGNORECASE)

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self._session.__exit__(exception_type, exception_value, traceback)

    def set_base_output_dir(self, directory):
        if os.name == 'nt':
            if directory.startswith('\\\\'):
                self._base_output_dir = '\\\\?\\UNC\\' + directory[2:]
            else:
                self._base_output_dir = '\\\\?\\' + directory
        else:
            self._base_output_dir = directory

    def login(self, username, password):
        """Login to the portal."""
        login_payload = { 'login[username]': username, 'login[password]': password }
        self._session.post(self.login_url, login_payload)

    def download_study_materials(self, num_workers=1):
        """Download all study videos and classnotes."""
        download_manager = None
        try:
            download_manager = self._DownloadManager(self._session, num_workers)
            download_manager.verbose = self.verbose

            result = self._get_with_retry(3, self.study_url)
            soup = BeautifulSoup(result.text, 'html.parser')
            nav = soup.find('div', id='portal-nav-secondary')
            self._download_study_materials_helper(download_manager, ['Lectures'], nav)

            print(f"Finished adding items to queue. Currently waiting for {download_manager.get_queue_size()} items to complete.")
            download_manager.finish_adding()
            download_manager.join()
        except KeyboardInterrupt:
            if download_manager:
                try:
                    print(f"Cancelling with {download_manager.get_queue_size()} items remaining to be downloaded.")
                    download_manager.cancel()
                    print("Waiting for existing downloads to finish.")
                    download_manager.join_on_workers()
                except KeyboardInterrupt:
                    print(f"Stopping with {download_manager.get_queue_size()} items remaining to be downloaded.")
                    download_manager.stop()
                    print("Waiting for threads to clean up.")
                    download_manager.join_on_workers_blocking()
        except:
            print(f"Got unexpected exception: {traceback.format_exc()}")
            if download_manager:
                print(f"Stopping with {download_manager.get_queue_size()} items remaining to be downloaded.")
                download_manager.stop()
                print("Waiting for threads to clean up.")
                download_manager.join_on_workers_blocking()

    def _download_study_materials_helper(self, download_manager, path, node):
        ul = node.find('ul', class_='submenu-list', recursive=False)
        for li in ul.find_all('li', class_=['leaf', 'leaf-selected'], recursive=False):
            # Get name
            item = li.find('div', class_=['subnav-item', 'subnav-item-selected-open'], recursive=False)
            name = item.a.string.strip()
            new_path = path + [name]
            # Does this have children?
            div = li.find('div', class_='subnav-item-child', recursive=False)
            if div:
                # Recurse until we get a true leaf node
                self._download_study_materials_helper(download_manager, new_path, div)
            else:
                url = urllib.parse.urljoin(self.study_url, item.a.get('href'))
                self._process_leaf(download_manager, url, new_path)

    def _process_leaf(self, download_manager, url, path):
        print(f"Processing {'/'.join(path)}.")
        result = self._get_with_retry(3, url)
        soup = BeautifulSoup(result.text, 'html.parser')
        # Video
        tag = soup.find('div', id='content-tab-video')
        if tag:
            self._download_videos(download_manager, url, path, tag)

        # Classnotes
        tag = soup.find('div', id='content-tab-classnotes')
        if tag:
            self._download_classnotes(download_manager, url, path, tag)

        # Additional Media Table
        tag = soup.find('table', class_='coloured-table')
        if tag:
            if path[-1] == 'Alumni Lectures':
                self._process_alumni_lectures(download_manager, url, path, tag)
            else:
                self._process_extra_media(download_manager, url, path, tag)

    def _download_videos(self, download_manager, url, path, div):
        path = path + ['Video']
        directory = os.path.join(self._base_output_dir, *[self._DownloadManager._sanitize_filename(n) for n in path])
        div = div.find('div', id='mediarecordingarea', recursive=False)
        for recording_entry in div.find_all('div', class_='recording-entry', recursive=False):
            # Skip LB (Low Bandwidth) versions
            if 'LB' in recording_entry.get('class'):
                continue
            link = recording_entry.find('a', class_='launchable_recording')
            video_id = link.get('id').replace('launchable_recording', '')
            split_url = url.split('/')
            portal_id = split_url[7]
            portal_node_id = split_url[-3]
            params = {
                'mediaRecordingId': video_id,
                'portalId': portal_id,
                'productPortalNodeId': portal_node_id
            }
            request_caller = 'videoJs'
            post_url = urllib.parse.urljoin(url, f"/erp_live/2/index.php/RecordingsPlayerV2/{request_caller}/")
            result = self._session.post(post_url, data=params, headers={'referer': url})
            if not result.ok:
                raise Exception(f"Failed to get video URL. params: {params}")
            json_result = result.json()
            soup = BeautifulSoup(json_result['html'], 'html.parser')
            video_url = soup.source.get('src')
            self._download_item(download_manager, directory, video_url, referrer=url)

    def _download_classnotes(self, download_manager, url, path, div):
        path = path + ['Classnotes']
        directory = os.path.join(self._base_output_dir, *[self._DownloadManager._sanitize_filename(n) for n in path])
        div = div.find('div', id='classnotes-area', recursive=False)
        for classnotes_link in div.find_all('div', class_='classnotes-link', recursive=False):
            link = classnotes_link.find('a')
            if link != -1:
                url_item = urllib.parse.urljoin(url, link.get('href'))
                self._download_item(download_manager, directory, url_item, referrer=url)

    def _process_alumni_lectures(self, download_manager, url, path, table):
        tbody = table.tbody or table
        for tr in tbody.find_all('tr', recursive=False):
            cells = tr.find_all('td', recursive=False)
            if len(cells) != 1:
                continue
            name = cells[0].string.strip()
            url_item = urllib.parse.urljoin(url, cells[0].a.get('href'))
            self._process_alumni_lectures_extra_media(download_manager, url_item, path + [name])

    def _process_alumni_lectures_extra_media(self, download_manager, url, path):
        request = self._get_with_retry(3, url)
        soup = BeautifulSoup(request.text, 'html.parser')
        for td1, td2 in zip(*[iter(soup.find_all('td'))]*2):
            name = td1.string.strip()
            new_url = urllib.parse.urljoin(url, td2.a.get('href'))
            self._process_7city_page(download_manager, new_url, path + [name])

    def _process_extra_media(self, download_manager, url, path, table):
        tbody = table.tbody or table
        for tr in tbody.find_all('tr', recursive=False):
            cells = tr.find_all('td', recursive=False)
            if len(cells) != 2:
                continue
            name = cells[0].string.strip()
            url_item = urllib.parse.urljoin(url, cells[1].a.get('href'))
            self._process_7city_page(download_manager, url_item, path + [name])

    def _process_7city_page(self, download_manager, url, path):
        print(f"Processing {'/'.join(path)}.")
        result = self._get_with_retry(3, url)
        soup = BeautifulSoup(result.text, 'html.parser')
        try:
            body = soup.html.body.html.body
        except AttributeError:
            body = None
        if body:
            file_types = []
            for div in body.find_all('div', class_=['filetypes', 'filetypeselected'], recursive=False):
                file_type = div.get('id').replace('_btn', '')
                if file_type not in self._IGNORED_7CITY_TYPES:
                    file_types.append(file_type)
            for file_type in file_types:
                new_path = path + [file_type]
                directory = os.path.join(self._base_output_dir, *[self._DownloadManager._sanitize_filename(n) for n in new_path])
                div = body.find('div', id=file_type, recursive=False)
                for cell in div.find_all('td'):
                    link = cell.a
                    description = cell.a.string
                    if 'low bandwidth' in description.lower():
                        continue
                    url_item = urllib.parse.urljoin(url, link.get('href'))
                    request = self._get_with_retry(3, url_item)
                    soup = BeautifulSoup(request.text, 'html.parser')
                    
                    # Check for meta refresh
                    meta = soup.find('meta', attrs={'http-equiv': 'refresh'})
                    if meta:
                        text = meta.get('content').split(';')[1]
                        if text.strip().lower().startswith('url='):
                            new_url = text[4:].strip()
                        else:
                            raise Exception("Found <meta> tag but no URL.")
                    else:
                        script = soup.html.body.script.text
                        new_url = urllib.parse.urljoin(url_item, script.replace("document.location.href='", '').replace("';", '').strip())
                    self._process_7city_url(download_manager, directory, url, new_url)
            return
        try:
            script = soup.html.body.script.text
            new_url = urllib.parse.urljoin(url, script.replace("document.location.href='", '').replace("';", '').strip())
            if new_url.startswith('https://erp.fitchlearning.com/erp_live/delegates/delegates_recording.php?emrid='):
                result = self._get_with_retry(3, new_url)
                soup = BeautifulSoup(result.text, 'html.parser')
                script = soup.html.body.script.text
                new_url = urllib.parse.urljoin(new_url, script.replace("document.location.href='", '').replace("';", '').strip())
        except AttributeError:
            new_url = None
        if new_url:
            directory = os.path.join(self._base_output_dir, *[self._DownloadManager._sanitize_filename(n) for n in path])
            self._process_7city_url(download_manager, directory, url, new_url)
            return
        raise Exception("Unknown page type. Could not find any URL or media to process.")

    def _process_7city_url(self, download_manager, directory, url, new_url):
        if self._regex_7city_html_video.match(new_url):
            self._process_7city_html_video(download_manager, directory, new_url)
        elif self._regex_ssrn.match(new_url):
            self._process_ssrn(download_manager, directory, new_url)
        elif self._regex_html.match(new_url):
            print(f"Skipping unknown URL type: {new_url}.")
        elif self._regex_webex.match(new_url):
            pass
        elif self._regex_whitelist.match(new_url):
            self._download_item(download_manager, directory, new_url, referrer=url)
        elif self._regex_mms.match(new_url):
            if self.verbose:
                print(f"Skipping streaming mms video. {new_url}")
            pass
        elif new_url in self._EXCLUDE_LIST:
            pass
        else:
            print(f"Downloading: {new_url}.")
            self._download_item(download_manager, directory, new_url, referrer=url)

    def _process_7city_html_video(self, download_manager, directory, url):
        result = self._get_with_retry(3, url)
        soup = BeautifulSoup(result.text, 'html.parser')
        video = soup.find('video')
        if video:
            new_url = urllib.parse.urljoin(url, video.source.get('src'))
            self._download_item(download_manager, directory, new_url, referrer=url)
            return
        div = soup.html.body.find('div', id='page', recursive=False)
        if div:
            a = div.find('a', id='player', recursive=False)
            if a:
                new_url = urllib.parse.urljoin(url, a.get('href'))
                self._download_item(download_manager, directory, new_url, referrer=url)
            return
        swf = soup.find('object', type='application/x-shockwave-flash')
        if swf:
            new_url = urllib.parse.urljoin(url, swf.get('data'))
            self._download_item(download_manager, directory, new_url, referrer=url)
            return

    def _process_ssrn(self, download_manager, directory, url):
        ssrn_id = self._regex_ssrn.search(url).group(1)
        path = os.path.join(directory, f'SSRN-id{ssrn_id}.pdf')
        if not os.path.isfile(path):
            print(f"Skipping SSRN paper. Please download manually: {url}.")

    def _download_item(self, download_manager, directory, url, referrer=None):
        filename = url.split('?')[0].rsplit('/', 1)[1]
        if self.verbose:
            print(f"Adding {filename} to download queue.")
        download_manager.enqueue(directory, filename, url, referrer)

    def _get_with_retry(self, max_attempts, url):
        retry_count = 0
        while True:
            try:
                time.sleep(0.5 * retry_count)
                return self._session.get(url)
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
                retry_count += 1
                if retry_count >= max_attempts:
                    raise


    class _DownloadManager:
        def __init__(self, session, num_workers):
            self._session = session
            self._cancellation_token = threading.Event()
            self._stop_token = threading.Event()
            self._num_workers = num_workers
            self._download_queue = queue.Queue()
            self._threads = []
            self.max_retry_count = 5
            # Chuck size in bytes
            self.streaming_chunk_size = 1024 * 1024
            self.overwrite = False
            self.verbose = False

            self._start_download_workers()

        def _start_download_workers(self):
            for _ in range(self._num_workers):
                t = threading.Thread(target=self._download_queue_worker)
                # Setting daemon = True means these threads will be terminated when main process exits
                #t.daemon = True
                self._threads.append(t)
                t.start()

        def join(self):
            """Blocks waiting for all downloads to complete."""
            # Don't use queue.join() because it ignores KeyboardInterrupts.
            i = 0
            while self._download_queue.unfinished_tasks > 0:
                if i % 10 == 0:
                    print(f"Number of items left in queue: {self._download_queue.unfinished_tasks}.")
                i += 1
                time.sleep(1)

        def join_on_workers(self):
            """Wait for all background workers to shut down. Use after calling cancel() or stop()."""
            num_alive = sum([1 if t.is_alive() else 0 for t in self._threads])
            i = 0
            while num_alive > 0:
                if i % 10 == 0:
                    print(f"Number of active downloads: {num_alive}.")
                i += 1
                time.sleep(1)
                num_alive = sum([1 if t.is_alive() else 0 for t in self._threads])

        def join_on_workers_blocking(self):
            """Blocks waiting for all background workers to shut down. Use after calling cancel() or stop()."""
            for t in self._threads:
                t.join()

        def cancel(self):
            """Instruct worker threads to stop after current download."""
            self._cancellation_token.set()

        def stop(self):
            """Instruct worker threads to stop current download."""
            self._stop_token.set()

        def enqueue(self, directory, filename, url, referrer):
            """Add a download to the queue."""
            self._download_queue.put(self._QueueItem(directory, filename, url, referrer))

        def finish_adding(self):
            """Call this when done adding items to the queue. This signals background workers can shut down."""
            for _ in range(self._num_workers):
                self._download_queue.put(None)

        def get_queue_size(self):
            """Get the current number of unfinished downloads (in queue and in progress)."""
            return self._download_queue.unfinished_tasks

        def _download_queue_worker(self):
            while not self._cancellation_token.is_set() and not self._stop_token.is_set():
                try:
                    queue_item = self._download_queue.get(block=True, timeout=1)
                except queue.Empty:
                    continue
                if queue_item == None:
                    self._download_queue.task_done()
                    break
                if self.verbose:
                    print(f"Dequeued {queue_item.filename} for processing.")
                try:
                    self._do_download_item(queue_item.directory, queue_item.filename, queue_item.url, queue_item.referrer)
                except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as exc:
                    queue_item.increment_failure_count()
                    print(f"Attempt {queue_item.failure_count} of {self.max_retry_count} to download {queue_item.filename}. Exception: {exc}.")
                    if queue_item.failure_count < self.max_retry_count:
                        time.sleep(0.5 * queue_item.failure_count)
                        if self.verbose:
                            print(f"Adding {queue_item.filename} back to queue.")
                        self._download_queue.put(queue_item)
                    else:
                        print(f"Giving up on {queue_item.filename}.")
                except:
                    # Do not let any exceptions get past here and terminate the thread
                    print(f"Exception downloading {queue_item.url}.\r\n{traceback.format_exc()}")
                self._download_queue.task_done()

        def _do_download_item(self, directory, filename, url, referrer):
            filename = self._sanitize_filename(filename)
            path = os.path.join(directory, filename)
            result = self._session.head(url, headers={'referer': referrer})
            if 'Last-Modified' in result.headers:
                last_mod = datetime(*eut.parsedate(result.headers['Last-Modified'])[:6])
                unix_time = (last_mod - datetime.utcfromtimestamp(0)).total_seconds()
            else:
                unix_time = 0
            if self.overwrite or not os.path.isfile(path) or os.path.getmtime(path) < unix_time:
                if self.verbose:
                    print(f"Downloading {filename}.")
                result = self._session.get(url, headers={'referer': referrer}, stream=True)
                result.raise_for_status()
                stopped_early = False
                self._make_dir(directory)
                with tempfile.NamedTemporaryFile(dir=directory, delete=False) as f:
                    for chunk in result.iter_content(chunk_size=self.streaming_chunk_size):
                        if self._stop_token.is_set():
                            stopped_early = True
                            break
                        if chunk:
                            f.write(chunk)
                if stopped_early:
                    self._remove_if_exists(f.name)
                    return
                os.replace(f.name, path)
                if unix_time:
                    self._change_file_time(path, unix_time)
            else:
                if self.verbose:
                    print(f"Skipping {filename} because it already exists.")

        @staticmethod
        def _change_file_time(filename, unix_time):
            os.utime(filename, (unix_time, unix_time))

        @staticmethod
        def _sanitize_filename(name):
            # Windows only, and not perfect
            name = re.sub(r'[\\\/:?"<>|]', '', name)
            name = re.sub(r'\.*$', '', name)
            return name

        @staticmethod
        def _make_dir(directory):
            try:
                os.makedirs(directory)
            except OSError as exc:
                if exc.errno != errno.EEXIST or not os.path.isdir(directory):
                    raise

        @staticmethod
        def _remove_if_exists(filename):
            try:
                os.remove(filename)
            except OSError as e:
                if e.errno != errno.ENOENT:
                    raise

        class _QueueItem:
            def __init__(self, directory, filename, url, referrer):
                self.directory = directory
                self.filename = filename
                self.url = url
                self.referrer = referrer
                self.failure_count = 0

            def increment_failure_count(self):
                """Increment failure count."""
                self.failure_count += 1


def main():
    import config
    username = config.SETTINGS['username']
    password = config.SETTINGS['password']
    outdir = config.SETTINGS['outdir']
    with CqfDownloader() as cqf:
        cqf.set_base_output_dir(outdir)
        cqf.login(username, password)
        cqf.download_study_materials(num_workers=8)


if __name__ == '__main__':
    main()
