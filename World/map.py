import os
import os.path as osp
import matplotlib.pyplot as plt
from matplotlib.patches import Circle, Polygon
import shapefile
import numpy as np

from config import PARAMS


class WorldMap(object):

    def __init__(self, map_name='ne_110m_land', params=PARAMS):
        self.map_name = map_name
        self.params = params

        self.world = shapefile.Reader(
            shp=open(osp.join(self.map_name, self.map_name + '.shp'), 'rb'),
            shx=open(osp.join(self.map_name, self.map_name + '.shx'), 'rb'),
            prj=open(osp.join(self.map_name, self.map_name + '.prj'), 'rb'),
        )
        self.globe = None

    @staticmethod
    def normalize_angle(angle):
        '''
        A method to normalize any angle to be in [-180,180)
        '''
        while angle >= 180:
            angle -= 360
        while angle < -180:
            angle += 360

        assert (angle >= -180) & (angle < 180) # checking that 'angle' is well-normalized

        return angle

    @staticmethod
    def project(coord, angle=0, turn=0, flip=False, r=1):
        '''
        Projects the coordinates on the 2D map 
        '''
        x, y = coord
        y = y*np.pi/180
        x = x - angle + turn*360
        unseen = False

        pos_x = r*np.sin(x*np.pi/180)*np.cos(y)
        pos_y = r*np.sin(y)
        d = pos_x**2 + pos_y**2

        if (x > 90) & (d <= 1):
            pos_x = r*np.cos(y)
            unseen = True
        elif (x < -90) & (d <= 1):
            pos_x = - r*np.cos(y)
            unseen = True

        if flip:
            pos_x = - pos_x

        return (pos_x, pos_y), unseen

    def set_figure(self):
        '''
        Reset the figure
        '''
        if hasattr(self, 'fig'):
            plt.close('all')

        # creating the general figure
        self.fig, self.ax = plt.subplots(figsize=[self.params['figure']['size']]*2)
        self.fig.subplots_adjust(left=0, right=1, bottom=0, top=1)
        self.ax.set_axis_off()
        extra = 1 + self.params['figure']['extra_space']
        self.ax.set_xlim(- extra, extra)
        self.ax.set_ylim(- extra, extra)

    def plot_globe(self, angle=0):
        '''
        Plot the globe and its shade
        '''
        angle = self.normalize_angle(angle)

        self.globe = Circle(
            xy=(0, 0),
            radius=1,
            color=self.params['globe']['water_colour'],
            zorder=self.params['zorder']['water'],
            lw=0,
        )
        self.ax.add_patch(self.globe)
        for shape in self.world.shapes():
            for turn in [-1, 0, 1]: # to cover for the boundary problems
                points, unseen = zip(*[self.project(point, angle, turn) for point in shape.points])
                if not all(unseen):
                    self.ax.add_patch(Polygon(
                        xy=points,
                        color=self.params['globe']['border_colour'],
                        zorder=self.params['zorder']['land_border'],
                        lw=self.params['globe']['border'],
                        clip_path=self.globe,
                        joinstyle='round',
                    ))
                    self.ax.add_patch(Polygon(
                        xy=points,
                        color=self.params['globe']['land_colour'],
                        zorder=self.params['zorder']['land'],
                        lw=0,
                    ))

        self.plot_shade(angle)

    def plot_shade(self, angle=0):
        '''
        Plot the shaded version of the globe
        '''
        angle = self.normalize_angle(angle + self.params['shade']['angle'])

        # transformation applied on the shade
        transform = self.ax.transData.get_affine()
        x_shift = transform.get_matrix()[0,2]
        y_shift = transform.get_matrix()[1,2]
        x_scale = transform.get_matrix()[0,0]
        y_scale = transform.get_matrix()[1,1]

        transform.get_matrix()[np.eye(3) != 1] = 0 ### TO BE CHANGED
        transform.scale(
            self.params['shade']['ratio']*self.params['shade']['scale'],
            self.params['shade']['scale']
        )
        transform.rotate_deg(self.params['shade']['rotation'])
        transform.translate(
            x_shift + x_scale*self.params['shade']['x_pos'],
            y_shift - y_scale + y_scale*self.params['shade']['y_pos']
        )

        # plotting the shaded world sphere
        self.ax.add_patch(Circle(
            xy=(0, 0),
            radius=1,
            color=self.params['shade']['water_colour'],
            zorder=self.params['zorder']['shade_water'],
            transform=transform,
            lw=0,
        ))
        for shape in self.world.shapes():
            for turn in [-1, 0, 1]: # to cover for the boundary problems
                points, unseen = zip(*[self.project(point, angle, turn, flip=True) for point in shape.points])
                if not all(unseen):
                    self.ax.add_patch(Polygon(
                        xy=points,
                        color=self.params['shade']['land_colour'],
                        zorder=self.params['zorder']['shade_land'],
                        transform=transform,
                        lw=0,
                    ))

    def savefig(self, name='map', folder='.'):
        '''
        Saves the current state of the figure
        '''
        assert hasattr(self, 'fig')
        if not osp.exists(folder):
            os.makedirs(folder)
        self.fig.savefig(osp.join(folder, name + '.png'))

    def plot(self, name='map', folder='.', angle=0):
        '''
        Plots the world globe
        '''
        self.set_figure()
        self.plot_globe(angle)
        self.savefig(name, folder)