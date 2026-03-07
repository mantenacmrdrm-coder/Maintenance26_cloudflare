'use client';
import {
  signInWithPopup,
  GoogleAuthProvider,
  signOut as firebaseSignOut,
} from 'firebase/auth';
import { initializeFirebase } from '../index';

export async function signInWithGoogle() {
  const { auth } = initializeFirebase();
  const provider = new GoogleAuthProvider();
  provider.setCustomParameters({ hl: 'fr' });
  auth.languageCode = 'fr';
  await signInWithPopup(auth, provider);
}

export async function signOut() {
  const { auth } = initializeFirebase();
  try {
    await firebaseSignOut(auth);
    // Reload to ensure all state is cleared
    window.location.href = '/login';
  } catch (error) {
    console.error('Error signing out', error);
  }
}
