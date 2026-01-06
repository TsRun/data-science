NAME		= graph

SRCS		= main.cpp \
			  Graph.cpp \
			  Vector2.cpp

OBJS		= $(SRCS:.cpp=.o)

CXX			= c++
CXXFLAGS	= -Wall -Wextra -Werror -std=c++98

all:		$(NAME)

$(NAME):	$(OBJS)
			$(CXX) $(CXXFLAGS) $(OBJS) -o $(NAME)

clean:
			rm -f $(OBJS)

fclean:		clean
			rm -f graph.png
			rm -f points.txt
			rm -f $(NAME)

re:			fclean all

.PHONY:		all clean fclean re
