import os
import os.path as osp
import matplotlib.pyplot as plt
from matplotlib.patches import *
from matplotlib.transforms import *
import shapefile
import numpy as np
import cv2
import pandas as pd


PARAMS = {
    'figure_size' : 10,
    'flat_figure_ratio' : 16/9,
    'land_colour' : 'forestgreen',
    'water_colour' : 'aquamarine',
}
SHADE_PARAMS = {
    'land_colour' : 'bisque',
    'water_colour' : 'linen',
    'dark_side_angle' : 170,
    'scale' : 0.7,
    'ratio' : 0.3,
    'angle' : 75,
    'x_pos' : 0.9,
    'y_pos' : 0.1,
}


def load_airport(folder='Dataset/Airport & Airline List'):
    '''
    106466 rows x 14 columns:
        'SK_LOCATION',
        'NK_LOCATION',
        'NM_LOCATIONTYPE',
        'CD_LOCATIONIATA',
        'CD_LOCATIONICAO',
        'NM_LOCATION',
        'NO_LATITUDE',
        'NO_LONGITUDE',
        'NK_ISOALPHA2COUNTRY',
        'CD_ISOALPHA3COUNTRY',
        'NM_REGIONIATA',
        'CD_REGIONSFO',
        'DT_VALIDFROM',
        'DT_VALIDTO'
    '''
    df = pd.read_csv(osp.join(folder, 'DIMLOCATION.csv'), delimiter=',')

    return df


class WorldMap(object):

    def __init__(self,
                 main_dir='World',
                 map_name='ne_110m_land',
                 frames_dir='frames',
                 params=PARAMS,
                 shade_params=SHADE_PARAMS):
        self.main_dir = main_dir
        self.map_name = map_name
        self.params = params
        self.shade_params = shade_params
        self.frames_dir = frames_dir
        if not osp.exists(osp.join(self.main_dir, self.frames_dir)):
            os.makedirs(osp.join(self.main_dir, self.frames_dir))

        self.world = shapefile.Reader(
            shp=open(osp.join(self.main_dir, self.map_name, self.map_name + '.shp'), 'rb'),
            shx=open(osp.join(self.main_dir, self.map_name, self.map_name + '.shx'), 'rb'),
            prj=open(osp.join(self.main_dir, self.map_name, self.map_name + '.prj'), 'rb'),
        )

        df = load_airport()
        self.airports = (df[['NO_LONGITUDE', 'NO_LATITUDE']]).dropna().sample(10,random_state=0).to_numpy()

    def plot(self, angle=0, name='map', folder=''):
        self.set_figure()
        globe = self.plot_world(angle)
        self.plot_airports(angle, globe)
        self.savefig(name, folder)

    def set_figure(self, extra=1):
        if hasattr(self, 'fig'):
            plt.close('all')

        # creating the general figure
        self.fig, self.ax = plt.subplots(figsize=(self.params['figure_size'], self.params['figure_size']))
        self.fig.subplots_adjust(left=0, right=1, bottom=0, top=1)
        self.ax.set_axis_off()
        self.ax.set_xlim(- (1 + extra), 1 + extra)
        self.ax.set_ylim(- (1 + extra), 1 + extra)

    def plot_world(self, angle=0):
        angle = self.normalize_angle(angle)
        assert (angle >= -180) & (angle < 180) # checking that 'angle' is well-normalized

        # Creating the globe
        globe = Circle((0, 0), 1, color=self.params['water_colour'], lw=0, zorder=0.5)
        self.ax.add_patch(globe)
        for shape in self.world.shapes():
            for turn in [-1, 0, 1]: # to cover for the boundary problems
                points, unseen = zip(*[self.spherized(point, angle, turn) for point in shape.points])
                if not all(unseen):
                    self.ax.add_patch(Polygon(points, color=self.params['land_colour'], lw=0, zorder=1))

        # plotting the shade
        new_angle = self.normalize_angle(angle + self.shade_params['dark_side_angle'])
        # transformation applied on the shade
        transform = self.ax.transData.get_affine()
        x_shift = transform.get_matrix()[0,2]
        y_shift = transform.get_matrix()[1,2]
        x_scale = transform.get_matrix()[0,0]
        y_scale = transform.get_matrix()[1,1]
        transform.get_matrix()[np.eye(3) != 1] = 0
        transform.scale(self.shade_params['ratio']*self.shade_params['scale'],self.shade_params['scale'])
        transform.rotate_deg(-self.shade_params['angle'])
        transform.translate(x_shift + x_scale*self.shade_params['x_pos'], y_shift - y_scale + y_scale*self.shade_params['y_pos'])

        # plotting the shaded world sphere
        self.ax.add_patch(Circle((0, 0), 1, color=self.shade_params['water_colour'], lw=0, zorder=-1.5, transform=transform))
        self.ax.add_patch(Circle((0, 0), 0.05, color=self.shade_params['water_colour'], lw=0, zorder=-1.5, transform=transform))
        for shape in self.world.shapes():
            for turn in [-1, 0, 1]: # to cover for the boundary problems
                points, unseen = zip(*[self.spherized(point, new_angle, turn, True) for point in shape.points])
                if not all(unseen):
                    self.ax.add_patch(
                        Polygon(points, color=self.shade_params['land_colour'], lw=0, zorder=-1, transform=transform)
                    )

        return globe

    def plot_airports(self, angle=0, globe=None):
        globe=None
        angle = self.normalize_angle(angle)
        assert (angle >= -180) & (angle < 180) # checking that 'angle' is well-normalized

        for turn in [-1, 0, 1]:
            for point in self.airports:
                point, unseen = self.spherized(point, angle, turn)
                if not unseen:
                    dist = (point[0]**2 + point[1]**2)**.5
                    a = np.arctan(point[1]/point[0])*180/np.pi
                    self.ax.add_patch(Ellipse(
                        point,
                        0.1*np.cos(dist*np.pi/2),
                        0.1,
                        a,
                        color='crimson',
                        lw=0,
                        zorder=1.5,
                        clip_path=globe
                    ))

    def savefig(self, name='map', folder=''):
        '''
        Saves the current state of the figure
        '''
        assert hasattr(self, 'fig')
        if not osp.exists(osp.join(self.main_dir, folder)):
            os.makedirs(osp.join(self.main_dir, folder))
        self.fig.savefig(osp.join(self.main_dir, folder, name + '.png'))

    @staticmethod
    def normalize_angle(angle):
        '''
        A method to normalize any angle to be in [-180,180)
        '''
        while angle >= 180:
            angle -= 360
        while angle < -180:
            angle += 360

        return angle

    @staticmethod
    def spherized(point, angle, turn, flip=False):
        x, y = point
        y = y*np.pi/180
        x = x - angle + turn*360
        unseen = False

        if x > 90:
            x = 90
            unseen = True
        elif x < -90:
            x = -90
            unseen = True

        x = np.sin(x*np.pi/180)*np.cos(y)
        y = np.sin(y)

        if flip:
            x = -x

        return (x, y), unseen

    def frames_to_video(self, name='world'):
        '''
        Transforms a directory of frames into a video.
        '''
        frames = [osp.join(self.main_dir, self.frames_dir, file) for file in sorted(os.listdir(osp.join(self.main_dir, self.frames_dir))) if file.endswith('.png')]

        h, w, _ = cv2.imread(frames[0]).shape

        video_file = osp.join(self.main_dir, name + '.avi')
        video = cv2.VideoWriter(
            video_file,
            cv2.VideoWriter_fourcc(*'XVID'),
            20,
            (w, h)
        )

        for frame in frames:
            image = cv2.imread(frame)
            video.write(image)

        video.release()
        cv2.destroyAllWindows()

    def make_frames(self, delta_angle=2):
        for angle in range(0, 360, delta_angle):
            print(angle)
            self.plot(angle, f'{angle:04d}', self.frames_dir)

if __name__ == '__main__':
    WM = WorldMap()
    #WM.plot()
    WM.make_frames()
    WM.frames_to_video()