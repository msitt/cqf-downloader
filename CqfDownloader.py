import email.utils as eut
import errno
import os
import queue
import shutil
import tempfile
import threading
import time
import urllib.parse
from datetime import datetime

import requests
from bs4 import BeautifulSoup


class CqfDownloader:
    """Class to download CQF materials."""
    def __init__(self, outdir):
        self._base_output_dir = outdir
        self._session = requests.Session()
        self.verbose = False

        self.login_url = 'https://erp.fitchlearning.com/erp_live/2/index.php/PortalEx/6/0/login/'
        self.study_url = 'https://erp.fitchlearning.com/erp_live/2/index.php/PortalEx/6/0/page/25776/show/'

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self._session.__exit__(exception_type, exception_value, traceback)

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

            result = self._session.get(self.study_url)
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
        result = self._session.get(url)
        soup = BeautifulSoup(result.text, 'html.parser')
        # Video
        div = soup.find('div', id='content-tab-video')
        if div:
            new_path = path + ['Video']
            self._download_videos(download_manager, url, new_path, div)

        # Classnotes
        div = soup.find('div', id='content-tab-classnotes')
        if div:
            new_path = path + ['Classnotes']
            self._download_classnotes(download_manager, url, new_path, div)

    def _download_videos(self, download_manager, url, path, div):
        directory = os.path.join(self._base_output_dir, *path)
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
            self._download_item(download_manager, directory, video_url, url)

    def _download_classnotes(self, download_manager, url, path, div):
        directory = os.path.join(self._base_output_dir, *path)
        div = div.find('div', id='classnotes-area', recursive=False)
        for classnotes_link in div.find_all('div', class_='classnotes-link', recursive=False):
            link = classnotes_link.find('a')
            if link != -1:
                url_item = urllib.parse.urljoin(url, link.get('href'))
                self._download_item(download_manager, directory, url_item, url)

    def _download_item(self, download_manager, directory, url, referrer=None):
        filename = url.split('?')[0].rsplit('/', 1)[1]
        if self.verbose:
            print(f"Adding {filename} to download queue.")
        download_manager.enqueue(directory, filename, url, referrer)


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
                except (requests.ConnectTimeout, requests.Timeout) as exc:
                    queue_item.increment_failure_count()
                    print(f"Attempt {queue_item.failure_count} of {self.max_retry_count} to download {queue_item.filename}. Exception: {exc}.")
                    if queue_item.failure_count < self.max_retry_count:
                        self._download_queue.put(queue_item)
                self._download_queue.task_done()

        def _do_download_item(self, directory, filename, url, referrer):
            full_path = os.path.join(directory, filename)
            if self.overwrite or not os.path.isfile(full_path):
                if self.verbose:
                    print(f"Downloading {filename}.")
                self._make_dir(directory)
                result = self._session.get(url, headers={'referer': referrer}, stream=True)
                stopped_early = False
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
                os.rename(f.name, full_path)
                last_mod = datetime(*eut.parsedate(result.headers['Last-Modified'])[:6])
                self._change_file_time(full_path, last_mod)
            else:
                if self.verbose:
                    print(f"Skipping {filename} because it already exists.")

        @staticmethod
        def _change_file_time(filename, date):
            unix_time = (date - datetime.utcfromtimestamp(0)).total_seconds()
            os.utime(filename, (unix_time, unix_time))

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

            def increment_failure_count(self, ):
                """Increment failure count."""
                self.failure_count += 1


def main():
    import config
    username = config.SETTINGS['username']
    password = config.SETTINGS['password']
    outdir = config.SETTINGS['outdir']
    with CqfDownloader(outdir) as cqf:
        cqf.login(username, password)
        cqf.download_study_materials(num_workers=8)


if __name__ == '__main__':
    main()
