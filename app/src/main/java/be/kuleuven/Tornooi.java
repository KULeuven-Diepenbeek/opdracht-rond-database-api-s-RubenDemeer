package be.kuleuven;

import java.util.ArrayList;

public class Tornooi {
  private int id;
  private String clubnaam;
  // For relations
  private ArrayList<Speler> ingeschreven_spelers;
  private ArrayList<Wedstrijd> wedstrijden;

  public Tornooi() {

  }

  public Tornooi(int id, String clubnaam) {
    this.id = id;
    this.clubnaam = clubnaam;
    // For relations
    this.ingeschreven_spelers = new ArrayList<>();
    this.wedstrijden = new ArrayList<>();
  }

  public int getId() {
    return id;
  }

  public String getClubnaam() {
    return clubnaam;
  }

  public void setClubnaam(String clubnaam) {
    this.clubnaam = clubnaam;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + id;
    result = prime * result + ((clubnaam == null) ? 0 : clubnaam.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    Tornooi other = (Tornooi) obj;
    if (id != other.id)
      return false;
    if (clubnaam == null) {
      if (other.clubnaam != null)
        return false;
    } else if (!clubnaam.equals(other.clubnaam))
      return false;
    return true;
  }

  @Override
  public String toString() {
    return "Tornooi [id=" + id + ", clubnaam=" + clubnaam + "]";
  }

  // For relations
  public ArrayList<Speler> getIngeschrevenSpelers() {
    return ingeschreven_spelers;
  }

  public ArrayList<Wedstrijd> getWedstrijden() {
    return wedstrijden;
  }

  public void addSpeler(Speler speler) {
    this.ingeschreven_spelers.add(speler);
  }

  public void addWedstrijd(Wedstrijd wedstrijd) {
    this.wedstrijden.add(wedstrijd);
  }

  public void deleteSpeler(int spelerId) {
    ingeschreven_spelers.removeIf(speler -> speler.getTennisvlaanderenId() == spelerId);
  }

  public void deleteWedstrijd(int wedstrijdId) {
    wedstrijden.removeIf(wedstrijd -> wedstrijd.getId() == wedstrijdId);
  }
}
