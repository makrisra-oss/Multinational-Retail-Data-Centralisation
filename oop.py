#Practical 1

import math

class Cylinder:
    def __init__(self, height, radius=1):
        self.height = height
        self.radius = radius
        self.surface_area = self.get_surface_area()
        self.volume = self.get_volume()

    def get_surface_area(self):
        lateral_area = 2 * math.pi * self.radius * self.height
        base_area = 2 * math.pi * self.radius ** 2
        total_surface_area = lateral_area + 2 * base_area
        return round(total_surface_area, 2)

    def get_volume(self):
        volume = math.pi * self.radius ** 2 * self.height
        return round(volume, 2)

    def print_surface_area(self):
        print(f"Surface Area: {self.surface_area}")

    def print_volume(self):
        print(f"Volume: {self.volume}")

    def print_both_values(self):
        self.print_surface_area()
        self.print_volume()

# Create a cylinder with height 3 and default radius 1
cylinder = Cylinder(height=3)

# Print both values
cylinder.print_both_values()

#Practical 2

