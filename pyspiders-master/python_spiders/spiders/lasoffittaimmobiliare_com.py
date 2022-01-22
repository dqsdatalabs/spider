import scrapy
from scrapy.http.request import Request
from ..loaders import ListingLoader
import requests


class LasoffittaimmobiliareComSpider(scrapy.Spider):
    name = 'lasoffittaimmobiliare_com'
    allowed_domains = ['lasoffittaimmobiliare.com']
    start_urls = ['https://www.lasoffittaimmobiliare.com/risultati.php?tipomediaz=locazione&mobile=0&zoneval=&provincia=&dettagliozone=&zone=&tipopag=&tipologia=appartamento&prezzomax=&mq=&codice=']
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'development'

    def parse(self, response):
        for appartment in response.css("div.portfolio-item"):
            url = 'https://www.lasoffittaimmobiliare.com/' + \
                appartment.css("h4 > a").attrib['href']
            yield Request(url,
                          callback=self.populate_item,
                          )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        title = response.css(
            'body > div.wrapper > section > div.page-content.padding-bottom-20.padding-top-60.titolosmt > div:nth-child(1) > div:nth-child(1) > div > div.col-md-12.padding-left-nullo > h1::text').get()

        description = response.css(
            "body > div.wrapper > section > div.page-content.padding-bottom-20.padding-top-60.titolosmt > div:nth-child(1) > div:nth-child(1) > div > p::text").get()

        feats = response.css(
            'body > div.wrapper > section > div.page-content.padding-bottom-20.padding-top-60.titolosmt > div:nth-child(1) > div:nth-child(1) > div > div.col-md-12.padding-left-nullo > ul > li')

        rent = None
        id = None
        rooms = None
        space = None
        for item in feats:
            if item.css('i.fa.fa-euro'):
                rent = item.css('li::text').get().replace('.', '')
            elif item.css('i.fa.fa-folder'):
                id = item.css('li::text').get().strip()
            elif item.css('i.fa.fa-home'):
                space = item.css('li::text').get().replace('MQ ', '').strip()
            if item.css('i.fa.fa-bed'):
                rooms = item.css('li::text').get().strip()

        images = response.css('img.ig::attr(src)').extract()

        ameneties = response.css(
            'div.heading-title-alt.text-left.padding-bottom-10.padding-top-10.margin-bottom-nullo.b-bottom-dettagli')

        parking = None
        bathrooms = None
        energy = None
        elevator = None
        terrace = None
        floor = None
        for item in ameneties:
            if "Classe energetica" in item.css('div > h5::text').get():
                energy = item.css('div > span::text').get()
            elif "Bagni" in item.css('div > h5::text').get():
                bathrooms = item.css('div > span::text').get()
            elif "Piano" in item.css('div > h5::text').get():
                floor = item.css('div > span::text').get()
            elif "ascensore" in item.css('div > h5::text').get():
                el = item.css('div > span::text').get()
                if "si" in el:
                    elevator = True
            elif "garage" in item.css('div > h5::text').get():
                elo = item.css('div > span::text').get()
                elo = elo[0]
                if int(elo) > 0:
                    parking = True
            elif "Terrazzo" in item.css('div > h5::text').get():
                eli = item.css('div > span::text').get()
                eli = eli[0]
                if int(eli) > 0:
                    terrace = True

        if "disponibile da" in description:
            available_date = description.split('disponibile da ')[-1]

        lat = response.xpath("//input[@id='lt']/@value").get()
        long = response.xpath("//input[@id='ln']/@value").get()

        responseGeocode = requests.get(
            f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={long},{lat}&f=pjson&distance=50000&outSR=")
        responseGeocodeData = responseGeocode.json()
        zipcode = responseGeocodeData['address']['Postal']
        city = responseGeocodeData['address']['City']
        address = responseGeocodeData['address']['Match_addr']

        # MetaData
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_id", id)
        item_loader.add_value("title", title)
        item_loader.add_value("description", description)

        # Property Details
        item_loader.add_value("property_type", "apartment")
        item_loader.add_value("square_meters", int(space))
        item_loader.add_value("room_count", rooms)
        item_loader.add_value("bathroom_count", bathrooms)
        item_loader.add_value("address", address)
        item_loader.add_value("city", city)
        item_loader.add_value("zipcode", zipcode)
        item_loader.add_value("floor", floor)
        item_loader.add_value("parking", parking)
        item_loader.add_value("terrace", terrace)
        item_loader.add_value("elevator", elevator)

        item_loader.add_value("energy_label", energy)
        item_loader.add_value("available_date", available_date)

        item_loader.add_value("latitude", lat)
        item_loader.add_value("longitude", long)

        # # Images
        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count", len(images))

        # Monetary Status
        item_loader.add_value("rent", int(rent))
        item_loader.add_value("currency", "CAD")

        # LandLord Details
        item_loader.add_value("landlord_name", 'VALERIA FABBRI')
        item_loader.add_value("landlord_phone", '051 6154856')
        item_loader.add_value(
            "landlord_email", 'info@lasoffittaimmobiliare.com')

        yield item_loader.load_item()
