import type { RxCollection } from 'rxdb';
/**
 * Like normal promiseWait()
 * but will skip the wait time if the online-state changes.
 */
export function awaitRetry(
  collection: RxCollection,
  retryTime: number
) {
  if (
      typeof window === 'undefined' ||
      typeof window !== 'object' ||
      typeof window.addEventListener === 'undefined' ||
      navigator.onLine
  ) {
      return collection.promiseWait(retryTime);
  }

  let listener: any;
  const onlineAgain = new Promise<void>(res => {
      listener = () => {
          window.removeEventListener('online', listener);
          res();
      };
      window.addEventListener('online', listener);
  });

  return Promise.race([
      onlineAgain,
      collection.promiseWait(retryTime)
  ]).then(() => {
      window.removeEventListener('online', listener);
  });
}