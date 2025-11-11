def combine_video_lists():
    # import the two csv files as pandas dataframes
    df1 = pd.read_csv('ca_video_list.csv', encoding='utf-8')

    df2 = pd.read_csv('int_video_list.csv', encoding='utf-8')

    # append df1 and df2 vertically, keeping 1 column
    df = pd.concat([df1, df2], axis=0)

    # save the combined dataframe to a new csv file
    df.to_csv('video_list.csv', index=False, encoding='utf-8')
    return df


def main():
    df = combine_video_lists()

if __name__ == "__main__":
    main()