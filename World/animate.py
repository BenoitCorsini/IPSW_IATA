import os
import os.path as osp
import cv2
import shutil

import numpy as np
import itertools
from config import PARAMS

from flights import WorldFlights


class WorldAnimation(WorldFlights):

    def __init__(self, frames_dir='frames', fps=20, **kwargs):
        super().__init__(**kwargs)
        self.frames_dir = frames_dir
        self.fps = fps

    def make_frames(self, n_angles=360, n_rotations=1, plot_airports=True, plot_flights=True):
        if n_angles*n_rotations > 10000:
            raise Exception('Too many frames for the video! You really want to see that earth spin don\'t ya?')

        print(f'Number of frames to be made: {n_angles*n_rotations}')

        if osp.exists(self.frames_dir):
            shutil.rmtree(self.frames_dir)
        os.makedirs(self.frames_dir)

        index = 0
        for _ in range(n_rotations):
            for i_angle in range(n_angles):
                angle = i_angle*360/n_angles

                self.set_figure()
                self.plot_globe(angle)
                if plot_airports:
                    self.plot_airports(angle)
                if plot_flights:
                    self.plot_flights(angle)
                self.savefig(f'{index:04d}', self.frames_dir)
                index += 1

    def frames_to_video(self, name='world', folder='.'):
        '''
        Transforms a directory of frames into a video.
        '''
        if not osp.exists(folder):
            os.makedirs(folder)

        frames = [osp.join(self.frames_dir, file) for file in sorted(os.listdir(self.frames_dir))]

        h, w, _ = cv2.imread(frames[0]).shape

        video_file = osp.join(folder, name + '.avi')
        video = cv2.VideoWriter(
            video_file,
            cv2.VideoWriter_fourcc(*'XVID'),
            self.fps,
            (w, h)
        )

        for frame in frames:
            image = cv2.imread(frame)
            video.write(image)

        video.release()
        cv2.destroyAllWindows()

    def make(self, name='world', folder='.', **kwargs):
        self.make_frames(**kwargs)
        self.frames_to_video(name, folder)


if __name__ == '__main__':
    N = 10
    np.random.seed(0)
    airports = {f'airport {i}': {'coord':(x,y)} for i, (x,y) in enumerate(zip(np.random.rand(N)*360 - 180,np.random.rand(N)*180 - 90))}
    flights = {(x,y): {'size':1} for x,y in itertools.product(airports, airports) if (x != y) & (np.random.rand() < 2/N)}

    WA = WorldAnimation(airports=airports, flights=flights, params=PARAMS)
    WA.make(n_rotations=2, n_angles=36)