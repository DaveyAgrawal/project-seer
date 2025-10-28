declare module 'shapefile' {
  export function open(path: string): Promise<{
    read(): Promise<{
      done: boolean;
      value?: {
        type: string;
        properties: any;
        geometry: any;
      };
    }>;
  }>;
}