package be.kuleuven;

import java.util.ArrayList;

public class Speler {
  private int tennisvlaanderenId;
  private String naam;
  private int punten;
  // For relations
  private ArrayList<Wedstrijd> wedstrijden;
  private ArrayList<Tornooi> tornooien;

  // Constructors
  public Speler() {
  }

  public Speler(int tennisvlaanderenId, String naam, int punten) {
    this.tennisvlaanderenId = tennisvlaanderenId;
    this.naam = naam;
    this.punten = punten;
    // For relations
    this.wedstrijden = new ArrayList<>();
    this.tornooien = new ArrayList<>();
  }

  public int getTennisvlaanderenId() {
    return tennisvlaanderenId;
  }

  public void setTennisvlaanderenId(int tennisvlaanderenId) {
    this.tennisvlaanderenId = tennisvlaanderenId;
  }

  public String getNaam() {
    return naam;
  }

  public void setNaam(String naam) {
    this.naam = naam;
  }

  public int getPunten() {
    return punten;
  }

  public void setPunten(int punten) {
    this.punten = punten;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + tennisvlaanderenId;
    result = prime * result + ((naam == null) ? 0 : naam.hashCode());
    result = prime * result + punten;
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
    Speler other = (Speler) obj;
    if (tennisvlaanderenId != other.tennisvlaanderenId)
      return false;
    if (naam == null) {
      if (other.naam != null)
        return false;
    } else if (!naam.equals(other.naam))
      return false;
    if (punten != other.punten)
      return false;
    return true;
  }

  @Override
  public String toString() {
    return "Speler [tennisvlaanderenId=" + tennisvlaanderenId + ", naam=" + naam + ", punten=" + punten + "]";
  }

  // For relations
  public ArrayList<Wedstrijd> getWedstrijden() {
    return wedstrijden;
  }

  public void addWedstrijd(Wedstrijd wedstrijd) {
    if (wedstrijd != null && !wedstrijden.contains(wedstrijd)) {
      wedstrijden.add(wedstrijd);
    }
  }

  public void deleteWedstrijd(int wedstrijdId) {
    wedstrijden.removeIf(wedstrijd -> wedstrijd.getId() == wedstrijdId);
  }

  public ArrayList<Tornooi> getTornooien() {
    return tornooien;
  }

  public void addTornooi(Tornooi tornooi) {
    if (tornooi != null && !tornooien.contains(tornooi)) {
      tornooien.add(tornooi);
    }
  }

  public void deleteTornooi(int tornooiId) {
    tornooien.removeIf(tornooi -> tornooi.getId() == tornooiId);
  }
}
