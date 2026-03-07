const firebaseConfig = {
  projectId: "studio-8110015021-2f16f",
  appId: "1:180872850026:web:6a272e8f39d26ac9ac7813",
  apiKey: "AIzaSyDaTdILx6so1SMS2PVowgMsX-EIIQRBHl0",
  authDomain: "studio-8110015021-2f16f.firebaseapp.com",
  messagingSenderId: "180872850026",
};

export function getFirebaseConfig() {
  if (!firebaseConfig.apiKey) {
    console.warn("Firebase config is not set. Please check your environment variables.");
  }
  return firebaseConfig;
}
