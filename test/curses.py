import logging
import curses
import time

class CursesHandler(logging.Handler):
    def __init__(self, screen):
        logging.Handler.__init__(self)
        self.screen = screen
    def emit(self, record):
        try:
            msg = self.format(record)
            screen = self.screen
            fs = "\n%s"
            if not _unicode: #if no unicode support...
                screen.addstr(fs % msg)
                screen.refresh()
            else:
                try:
                    if (isinstance(msg, unicode) ):
                        ufs = u'\n%s'
                        try:
                            screen.addstr(ufs % msg)
                            screen.refresh()
                        except UnicodeEncodeError:
                            screen.addstr((ufs % msg).encode(code))
                            screen.refresh()
                    else:
                        screen.addstr(fs % msg)
                        screen.refresh()
                except UnicodeError:
                    screen.addstr(fs % msg.encode("UTF-8"))
                    screen.refresh()
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)


try:
    _unicode = True
    screen = curses.initscr()
    screen.nodelay(1)
    maxy, maxx = screen.getmaxyx()
    begin_x = 2; begin_y = maxy-5
    height = 5; width = maxx-4
    win = curses.newwin(height, width, begin_y, begin_x)
    curses.setsyx(-1, -1)
    screen.addstr("Testing my curses app")
    screen.refresh()
    win.refresh()
    win.scrollok(True)
    win.idlok(True)
    win.leaveok(True)
    mh = CursesHandler(win)
    formatter = logging.Formatter('%(asctime) -25s - %(name) -15s - %(levelname) -10s - %(message)s')
    formatterDisplay = logging.Formatter('%(asctime)-8s|%(name)-12s|%(levelname)-6s|%(message)-s', '%H:%M:%S')
    mh.setFormatter(formatterDisplay)
    logger = logging.getLogger('myLog')
    logger.addHandler(mh)


    for i in range(10):
        logger.error('message ' + str(i))
        time.sleep(1)


    curses.curs_set(1)
    curses.nocbreak()
    curses.echo()
    curses.endwin()

except NameError:
    _unicode = False