'use client';

import React from 'react';
import { initializeFirebase } from './index';
import { FirebaseProvider } from './provider';

export const FirebaseClientProvider = ({
  children,
}: {
  children: React.ReactNode;
}) => {
  const firebaseInstances = initializeFirebase();

  return (
    <FirebaseProvider value={firebaseInstances}>
      {children}
    </FirebaseProvider>
  );
};
