import scrapy
from python_spiders.helper import remove_white_spaces
from python_spiders.loaders import ListingLoader
import requests

class IdeeresidenzialiSpider(scrapy.Spider):
    name = 'ideeresidenziali'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['old.ideeresidenziali.com']

    def start_requests(self):
        start_urls = ['https://old.ideeresidenziali.com/strutture_categoria_tp.php?tipo=Affitto',
        'https://old.ideeresidenziali.com/strutture_categoria_tp.php?tipo=Affitto&page=2']

        for url in start_urls:
            yield scrapy.Request(url=url, callback=self.parse)





    def parse(self, response):
        for url in response.css("h4 a.color1::attr(href)").getall():
            yield scrapy.Request(url=url, callback=self.parse_page)

    def parse_page(self, response):
        aparts = ['APPARTAMENTI']
        houses = ['VILLE']


        title   = response.css(".title1 ::text").get().strip()
        property_type = response.css("div#left_col div p.date em ::text").re("Categorie:\W*[a-zA-Z0-9]+")
        address = response.xpath('//strong[contains(text(), "Indirizzo")]/following-sibling::text()').get()
        rent = response.xpath('//strong[contains(text(), "Prezzo")]/following-sibling::text()').get().replace("€","")
        description = remove_white_spaces(response.css("div#left_col div p.date em p::text").get())
        bathroom_count = int(float(response.xpath('//strong[contains(text(), "N. Bagni")]/following-sibling::text()').re("[0-9]+")[0].strip()))
        room_count = int(float(response.xpath('//strong[contains(text(), "N. Camere")]/following-sibling::text()').re("[0-9]+")[0].strip()))
        furnished = response.xpath('//strong[contains(text(), "Arredamento")]/following-sibling::text()').get().strip().lower() == "si"
        parking = response.xpath('//strong[contains(text(), "Garage")]/following-sibling::text()').get().strip()
        images = response.css("#galleria a::attr(href)").getall()
        latitude, longitude = response.css("script").re("([0-9]+.[0-9]+,\W*[0-9]+.*[0-9]+)")[0].split(",")[0].strip(),response.css("script").re("([0-9]+.[0-9]+,\W*[0-9]+.*[0-9]+)")[0].split(",")[1].strip()
        

        responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
        responseGeocodeData = responseGeocode.json()
        city = responseGeocodeData['address']['City']
        address = responseGeocodeData['address']['Match_addr']
        zipcode = responseGeocodeData['address']['Postal']

        if parking:
            if parking.lower() != 'no':
                parking = True
            else:
                parking = False

        if address:
            address = address.strip()


        if property_type:
            property_type = property_type[0].split(":")[1].strip()
            for apart in aparts:
                if apart in property_type:
                    property_type = 'apartment'


            for house in houses:
                if house in property_type:
                    property_type = 'house'

        if rent:
            rent = int(rent.strip())

            item = ListingLoader(response=response)
            item.add_value("external_source"        ,self.external_source)
            item.add_value("external_link"          ,response.url)
            item.add_value("title"                  ,title)
            item.add_value("address"                ,address)
            item.add_value("city"                   ,city)
            item.add_value("rent"                   ,rent)
            item.add_value("images"                 ,images)
            item.add_value("room_count"             ,room_count)
            item.add_value("bathroom_count"         ,bathroom_count)
            item.add_value("property_type"          ,property_type)
            item.add_value("parking"                ,parking)
            item.add_value("description"            ,description)
            item.add_value("currency"               ,"EUR")
            item.add_value("furnished"              ,furnished)
            item.add_value("latitude"               ,latitude)
            item.add_value("longitude"              ,longitude)
            item.add_value("zipcode"                ,zipcode)

            item.add_value("landlord_name"          ,'OLBIA Offices')
            item.add_value("landlord_email"         ,'info@ideeresidenziale.it')
            item.add_value("landlord_phone"         ,'0789 39256')

            yield item.load_item()


