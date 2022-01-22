import scrapy
from scrapy.http.request import Request
from ..loaders import ListingLoader
import requests
import re


class PaulinItSpider(scrapy.Spider):
    name = 'paulin_it'
    allowed_domains = ['paulin.it']
    start_urls = [
        'https://www.paulin.it/home/search/immobile_prezzo/ASC/0/0/1/0/0/0/0/1']
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    def parse(self, response):
        for appartment in response.css("body > div.body_wrap > div.middle > div > div.grid_8.content > div.re-list > div"):
            url = appartment.css("div.re-top > h2 > a").attrib['href']
            yield Request(url,
                          callback=self.populate_item,
                          )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        title = response.css(
            'body > div.body_wrap > div.middle > div > div.grid_8.content > div > h1::text').get()

        if 'CAPANNONE' in title:
            return

        rent = response.css('.re-price strong::text').get().split("€ ")[1].strip()
        try:
            if "." in rent:
                rent = rent.split(".")
                rent = rent[0]+rent[1]
        except:
            rent = rent

        some_feats = response.css(
            'body > div.body_wrap > div.middle > div > div.grid_8.content > div > div:nth-child(2) > div > em::text').extract()

        space = None
        rooms = 1
        bathrooms = None
        for itemaya in some_feats:
            if "mq" in itemaya:
                space = int(itemaya.split(' mq')[0].strip())
            elif "camere " in itemaya:
                rooms = itemaya.strip().split('camere ')[1].strip()
            elif "bagni" in itemaya:
                bathrooms = itemaya.strip()[-1]
                try:
                    bathrooms = int(bathrooms)
                except:
                    bathrooms = None
        if 'n' in rooms:
            rooms = 1

        description = response.css(".re-description ::text").extract()
        description = description[3:]




        if "ufficio" in description:
            return

        images = response.css('#rePhoto > li > a > img::attr(src)').extract()

        feats = response.css('ul.split_list > li')

        elevator = None
        furnished = None
        energy = None
        floor = None
        parking = None
        terrace = None
        for item in feats:
            if "Ascensore:" in item.css('strong::text').get():
                if 'si' in item.css('li::text').get():
                    elevator = True
                else:
                    elevator = False
            elif "Arredato" in item.css('strong::text').get():
                if 'si' in item.css('li::text').get():
                    furnished = True
                else:
                    furnished = False
            elif "Piano" in item.css('strong::text').get():
                floor = item.css('li::text').get()
            elif "Classe energetica" in item.css('strong::text').get():
                if 'n.d.' in item.css('li::text').get():
                    pass
                else:
                    energy = item.css('li::text').get()
            elif "Posto auto" in item.css('strong::text').get():
                if 'Scoperto' in item.css('li::text').get():
                    parking = False
                else:
                    pass
            elif "Terrazze" in item.css('strong::text').get():
                if '1' in item.css('li::text').get():
                    terrace = True
                else:
                    pass

        coords = response.xpath('/html/body/div[2]/div[3]/script/text()').get()

        lat = coords.split('lat: parseFloat(')[1].split(')')[0]
        lng = coords.split('lng: parseFloat(')[1].split(')')[0]

        responseGeocode = requests.get(
            f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={lng},{lat}&f=pjson&distance=50000&outSR=")
        responseGeocodeData = responseGeocode.json()
        zipcode = responseGeocodeData['address']['Postal']
        city = responseGeocodeData['address']['City']
        address = responseGeocodeData['address']['Match_addr']

        id = response.css('div.inner > b::text').get()

        # MetaData
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_id", id)
        item_loader.add_value("title", title)
        item_loader.add_value("description", description)

        # Property Details
        item_loader.add_value("property_type", "apartment")
        item_loader.add_value("square_meters", space)
        item_loader.add_value("room_count", rooms)
        item_loader.add_value("bathroom_count", bathrooms)
        item_loader.add_value("address", address)
        item_loader.add_value("city", city)
        item_loader.add_value("floor", floor)
        item_loader.add_value("zipcode", zipcode)
        # item_loader.add_value("available_date", avaialble_date)
        item_loader.add_value("parking", parking)
        item_loader.add_value("elevator", elevator)
        item_loader.add_value("furnished", furnished)
        item_loader.add_value("terrace", terrace)

        item_loader.add_value("energy_label", energy)

        item_loader.add_value("latitude", lat)
        item_loader.add_value("longitude", lng)

        # Images
        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count", len(images))

        # Monetary Status
        item_loader.add_value("rent", int(rent))
        # item_loader.add_value("utilities", utils)
        item_loader.add_value("currency", "EUR")

        # # LandLord Details
        item_loader.add_value("landlord_name", 'GRUPPO IMMOBILIARE PAULIN')
        item_loader.add_value("landlord_phone", '0432.511950')
        item_loader.add_value("landlord_email", 'info@paulin.it')

        yield item_loader.load_item()
