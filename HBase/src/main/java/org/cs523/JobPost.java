package org.cs523;

public class JobPost {
    private String id;
    private String title;

    public JobPost(String id, String title) {
        this.id = id;
        this.title = title;
    }

    public String getId() {
        return this.id;
    }

    public String getTitle() {
        return this.title;
    }
}
