import data from './equipment-images.json';

export type EquipmentImage = {
  matricule: string;
  imageUrl: string;
  imageHint: string;
};

export const EquipmentImages: EquipmentImage[] = data.equipmentImages;
