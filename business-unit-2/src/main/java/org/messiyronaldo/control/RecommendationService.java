package org.messiyronaldo.control;

public class RecommendationService {
    public String generateRecommendation(double pricePVPC, double cloudiness, String partOfDay) {
        // Solo recomendar placas solares durante el día
        if (!"d".equals(partOfDay)) {
            return "Es de noche - Usar luz normal ⚡";
        }

        // Lógica mejorada de recomendación
        if (pricePVPC > 150) {
            return cloudiness < 30 ? "Precio ALTO y buen clima - USAR PLACAS 📡⭐" :
                    cloudiness < 60 ? "Precio ALTO pero nublado - Usar placas con reservas 📡☁" :
                            "Precio ALTO pero muy nublado - Considerar usar red ⚡☁";
        } else if (pricePVPC > 100) {
            return cloudiness < 40 ? "Precio medio y buen clima - USAR PLACAS 📡" :
                    "Precio medio y nublado - Usar red o placas según necesidad ⚡/📡";
        } else {
            return "Precio BAJO - Usar luz normal ⚡";
        }
    }
}