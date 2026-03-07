'use client';

import { useState } from 'react';
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from '@/components/ui/card';
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert';
import { Loader2, Construction, LogOut, Info } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { signInWithGoogle, signOut } from '@/firebase/auth/auth';
import { useUser } from '@/firebase/auth/use-user';
import { Avatar, AvatarFallback, AvatarImage } from '@/components/ui/avatar';

function GoogleIcon(props: React.SVGProps<SVGSVGElement>) {
  return (
    <svg
      xmlns="http://www.w3.org/2000/svg"
      width="1em"
      height="1em"
      viewBox="0 0 48 48"
      {...props}
    >
      <path
        fill="#FFC107"
        d="M43.611 20.083H42V20H24v8h11.303c-1.649 4.657-6.08 8-11.303 8c-6.627 0-12-5.373-12-12s5.373-12 12-12c3.059 0 5.842 1.154 7.961 3.039l5.657-5.657C34.046 6.053 29.268 4 24 4C12.955 4 4 12.955 4 24s8.955 20 20 20s20-8.955 20-20c0-1.341-.138-2.65-.389-3.917z"
      ></path>
      <path
        fill="#FF3D00"
        d="M6.306 14.691c-1.645 3.119-2.656 6.641-2.656 10.309C3.65 28.66 4.661 32.182 6.306 35.309C9.096 30.54 16.03 24 24 24c-2.923 0-5.632.709-7.961 1.961l-5.733-5.733z"
      ></path>
      <path
        fill="#4CAF50"
        d="M24 48c5.166 0 9.86-1.977 13.409-5.192l-6.19-5.238C29.211 39.904 26.715 41 24 41c-7.97 0-14.904-5.458-17.694-12.691l-5.733 5.733C4.14 39.043 13.29 48 24 48z"
      ></path>
      <path
        fill="#1976D2"
        d="M43.611 20.083L43.595 20H24v8h11.303a12.016 12.016 0 0 1-4.996 7.82l6.19 5.238C42.062 35.845 44 30.138 44 24c0-1.341-.138-2.65-.389-3.917z"
      ></path>
    </svg>
  );
}

export function LoginView() {
  const [isLoading, setIsLoading] = useState(false);
  const user = useUser();

  const handleLogin = async () => {
    setIsLoading(true);
    try {
        await signInWithGoogle();
        // The useUser hook will trigger a re-render upon successful login
    } catch (error) {
        console.error("Firebase sign-in error", error);
    } finally {
        setIsLoading(false);
    }
  };
  
  const handleLogout = async () => {
      setIsLoading(true);
      await signOut();
      setIsLoading(false);
  }

  return (
    <div className="space-y-8">
        <Alert>
          <Info className="h-4 w-4" />
          <AlertTitle>Accès simplifié pour le développement</AlertTitle>
          <AlertDescription>
            Pour vous débloquer, la connexion obligatoire a été temporairement désactivée. Vous pouvez naviguer dans l'application sans être connecté. Cette page vous permet de tester le statut de l'authentification.
          </AlertDescription>
        </Alert>
      <Card className="w-full max-w-md mx-auto">
        <CardHeader className="text-center">
          <div className="mx-auto mb-4 w-fit rounded-lg bg-primary p-3 text-primary-foreground">
            <Construction className="h-8 w-8" />
          </div>
          <CardTitle>Maintenance Hub</CardTitle>
          <CardDescription>
            Statut de l'authentification
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          {user === null && <Loader2 className="mx-auto h-6 w-6 animate-spin" />}
          
          {user && (
             <div className="space-y-4">
                <div className="flex items-center gap-3 rounded-md border p-3">
                    <Avatar className="h-12 w-12">
                      <AvatarImage src={user.photoURL || ''} alt={user.displayName || ''} />
                      <AvatarFallback>{user.displayName?.charAt(0) || 'U'}</AvatarFallback>
                    </Avatar>
                    <div className="flex-1 overflow-hidden">
                      <p className="truncate font-semibold">{user.displayName}</p>
                      <p className="truncate text-sm text-muted-foreground">{user.email}</p>
                    </div>
                </div>
                 <Button onClick={handleLogout} disabled={isLoading} className="w-full" variant="outline">
                    {isLoading ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <LogOut className="mr-2 h-4 w-4" />}
                    Se déconnecter
                 </Button>
            </div>
          )}

          {user === false && (
            <Button onClick={handleLogin} disabled={isLoading} className="w-full">
                {isLoading ? (
                <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                ) : (
                <GoogleIcon className="mr-2 h-5 w-5" />
                )}
                Se connecter avec Google
            </Button>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
