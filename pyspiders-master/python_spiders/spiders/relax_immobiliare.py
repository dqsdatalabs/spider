import scrapy
from scrapy import Request
from ..items import ListingItem
from ..loaders import ListingLoader
import requests

class RelaxImmobiliareSpider(scrapy.Spider):
    name = 'relax_immo'
    allowed_domains = ['relaximmobiliare.com']
    start_urls = ['https://relaximmobiliare.com/index.php?cerca=immobile&elenco=immobiliAbiLoc&delete_id_search=1&id_search=']
    execution_type = 'development'
    country = 'italy'
    locale ='it'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    images = []

    def parse(self, response):
        for start_url in self.start_urls:
            yield Request(url=start_url,
                          callback=self.parse_area)

    def parse_area(self, response):
        area_urls = response.css('.btn-danger::attr(href)').extract()
        area_urls = [x.replace('.','https://relaximmobiliare.com',1) for x in area_urls]
        for area_url in area_urls:
            yield Request(url=area_url,
                          callback=self.parse_pages)

    def parse_pages(self, response):
        items = ListingItem()
        external_link = str(response.request.url)
        description = response.css(".col-md-6:nth-child(1)")[0].extract()
        description = description.split("</h3>")[1].split("<hr>")[0]
        terrace = None
        balcony = None
        if 'terraz' in description:
            terrace = True
        if 'balcon' in description:
            balcony = True
        external_id = response.css(".catAbitativoAffitto small::text")[0].extract()
        external_id = external_id[12:].strip()
        address = response.css("h3+ p::text")[0].extract()
        address = address.strip()

        list = response.css("p::text ,p strong::text").extract()

        property_index = [i for i,x in enumerate(list) if "Sotto tipologia" in x][0]
        property_type = list[property_index+1].strip()
        if "camera/e" in property_type:
            property_type = "room"
        else:
            property_type = "apartment"

        square_index = [i for i, x in enumerate(list) if "Dimensioni " in x][0]
        square_meters = list[square_index + 1].strip()
        try:
            square_meters = int(square_meters)
        except:
            square_meters = 0

        floor_index = [i for i, x in enumerate(list) if "Piano" in x][0]
        floor = list[floor_index + 1].strip()
        if any(char.isdigit() for char in floor):
            floor = ''.join(x for x in floor if x.isdigit())

        utilities_index = [i for i, x in enumerate(list) if "Spese Condominio" in x][0]
        utilities = list[utilities_index + 1].strip()
        try:
            utilities = int(utilities[1:3])
        except:
            utilities = 0

        elevator_index = [i for i, x in enumerate(list) if "Ascensore" in x][0]
        elevator = list[elevator_index + 1].strip()
        if "si" in elevator:
            elevator = True
        else:
            elevator = False

        furnish_index = [i for i, x in enumerate(list) if "Arredamento" in x][0]
        furnished = list[furnish_index + 1].strip()
        if "arredato" in furnished:
            furnished = True
        else:
            furnished = False

        parking_index = [i for i, x in enumerate(list) if "Posto Auto" in x][0]
        parking = list[parking_index + 1].strip()
        if "si" in parking:
            parking = True
        else:
            parking = False

        energy_label = response.css(".caratteristiche_immobile ::text").extract()
        energy_label = ' '.join(energy_label)
        energy_label = energy_label.split("Classe energetica")[1].strip()

        responseGeocode = requests.get(
            f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/find?f=json&text={address}&maxLocations=1")
        responseGeocodeData = responseGeocode.json()

        longitude = responseGeocodeData['locations'][0]['feature']['geometry']['x']
        latitude = responseGeocodeData['locations'][0]['feature']['geometry']['y']

        responseGeocode = requests.get(
            f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
        responseGeocodeData = responseGeocode.json()
        zipcode = responseGeocodeData['address']['Postal']
        city = responseGeocodeData['address']['City']
        address = responseGeocodeData['address']['Match_addr']
        longitude = str(longitude)
        latitude = str(latitude)

        rent = response.css("h2::text")[0].extract()
        rent = int(rent.split(" € ")[1])

        items['external_source'] = self.external_source
        items['external_link'] = external_link
        items['external_id'] = external_id
        items['address'] = address
        items['city'] = city
        items['zipcode'] = zipcode
        items['latitude'] = latitude
        items['longitude'] = longitude
        items['description'] = description
        items['property_type'] = property_type
        if square_meters != 0:
            items['square_meters'] = square_meters
        items['floor'] = floor
        items['balcony'] = balcony
        items['terrace'] = terrace
        items['utilities'] = utilities
        items['elevator'] = elevator
        items['furnished'] = furnished
        items['parking'] = parking
        if "Non Classificato" not in energy_label:
            items['energy_label'] = energy_label
        items['rent'] = rent
        items['currency'] = "EUR"
        items['landlord_name'] = "Relax Immobiliare"
        items['landlord_phone'] = "0854503233"
        items['landlord_email'] = "richieste.ri@gmail.com"

        yield items
