# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from  geopy.geocoders import Nominatim
from html.parser import HTMLParser 

class MySpider(Spider):
    name = 'orpi_com_ys_immobilier'
    execution_type='testing'
    country='france'
    locale='fr'

    def start_requests(self):

        start_urls = [
            {
                "url" : "https://www.orpi.com/recherche/ajax/rent?realEstateTypes%5B%5D=maison&agency=Ys-immobilier&sort=date-down&layoutType=mixte",
                "property_type" : "house",
                "annonce" : "location"
            },
            {
                "url" : "https://www.orpi.com/recherche/ajax/rent?realEstateTypes%5B%5D=appartement&agency=Ys-immobilier&sort=date-down&layoutType=mixte",
                "property_type" : "apartment",
                "annonce" : "location"
            },
        ] #LEVEL-1

        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={
                                     'property_type': url.get('property_type'),
                                     "annonce" : url.get("annonce"),
                                 })


    # 1. FOLLOWING
    def parse(self, response):

        # url = "https://www.orpi.com/annonce-location-maison-t1-le-havre-76600-b-e1l04e/"

        # yield Request(
        #     url, 
        #     callback=self.populate_item, 
        #     meta={
        #         "property_type" : response.meta.get("property_type"),
        #         "lat" : "bb",
        #         "lng" : "aa",
        #         "images" : "cc",
        #     },
        # )
        data = json.loads(response.body)
        for item in data["items"]:
            lat, lng = item["latitude"], item["longitude"]
            f_url = "https://www.orpi.com/annonce-" + response.meta.get("annonce") + "-" + item["slug"]
            images = item["images"]
            yield Request(
                f_url, 
                callback=self.populate_item, 
                meta={
                    "property_type" : response.meta.get("property_type"),
                    "lat" : lat,
                    "lng" : lng,
                    "images" : images,
                },
            )
        
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("latitude", response.meta.get('lat'))
        item_loader.add_value("longitude", response.meta.get('lng'))
        item_loader.add_value("images", response.meta.get('images'))

        item_loader.add_value("external_link", response.url)
        item_loader.add_xpath("title", "//title/text()")

        item_loader.add_value("external_source", "Orpiys_PySpider_"+ self.country + "_" + self.locale)

        latitude_longitude = response.xpath("//script[contains(.,'latitude')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('latitude: "')[1].split('"')[0].strip().replace('-', '.')
            longitude = latitude_longitude.split('longitude: "')[1].split('"')[0].strip().replace('-', '.')

            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        address = "".join(response.xpath("//section[@id='estate-map']/div/div/h2/text()").extract())
        if address:
            item_loader.add_value("address", address)

        # item_loader.add_xpath("deposit","substring-before(substring-after(//ul[contains(@class,'u-list-unstyled')]/li[contains(.,'Dépôt de garantie')],': '),'€')")
        
        item_loader.add_xpath("bathroom_count","substring-before(//li[@class='o-grid__col']/span/span[contains(.,'salle')]/text(),' ')")

        deposit = response.xpath("substring-before(substring-after(//ul[contains(@class,'u-list-unstyled')]/li[contains(.,'Dépôt de garantie')],': '),'€')").extract_first()
        if deposit:
            dep =deposit.replace("\xa0","")
            item_loader.add_value("deposit",dep)

        utilities = response.xpath("//div[@class='o-container']//p[contains(.,'Charges locatives')]//text()").get()
        if utilities:
            uti = utilities.replace("\xa0","").replace(",",".")
            uti=uti.split("Charges locatives")[1].split("euros ")[0]
            if "-" not in uti:
                item_loader.add_value("utilities",uti)



        square_meters = response.xpath("//script[contains(.,'latitude')]/text()").get()
        if square_meters:
            square_meters = str(int(float(square_meters.split('surfaceBien: "')[1].split('"')[0].strip().replace('-', '.'))))
            if square_meters != '0':
                item_loader.add_value("square_meters", square_meters)

        room_count = response.xpath("//script[contains(.,'latitude')]/text()").get()
        if room_count:
            room_count = room_count.split('nbChambres: "')[1].split('"')[0].strip().replace('-', '.')
            if room_count != '0':
                item_loader.add_value("room_count", room_count)
            else:
                item_loader.add_xpath("room_count", "//h1/span[2]/b[1]/text()")

        rent = response.xpath("//script[contains(.,'latitude')]/text()").get()
        if rent:
            rent = rent.split("local_totalvalue: '")[1].split("'")[0].strip().replace('-', '.')
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", 'EUR')

        external_id = response.xpath("//script[contains(.,'latitude')]/text()").get()
        if external_id:
            external_id = external_id.split("ProductID: '")[1].split("'")[0].strip()
            item_loader.add_value("external_id", external_id)

        description = response.xpath("//h2[contains(.,'Détails')]/following-sibling::p[1]/text()").getall()
        desc_html = ''      
        if description:
            for d in description:
                desc_html += d.strip() + ' '
            desc_html = desc_html.replace('\xa0', '')
            filt = HTMLFilter()
            filt.feed(desc_html)
            item_loader.add_value("description", filt.text)

        city = response.xpath("//script[contains(.,'latitude')]/text()").get()
        if city:
            city = city.split('nomVille: "')[1].split('"')[0].strip().replace('-', ' ')
            item_loader.add_value("city", city)

        energy_label = response.xpath("//script[contains(.,'latitude')]/text()").get()
        if energy_label:
            energy_label = energy_label.split('dpe: "')[1].split('"')[0].strip()
            energy_table = {'1': 'A', '2': 'B', '3': 'C', '4': 'D', '5': 'E', '6': 'F', '7': 'G'}
            if energy_label != '0':
                energy_label = energy_table[energy_label]
                item_loader.add_value("energy_label", energy_label)
        
        furnished = response.xpath("//script[contains(.,'latitude')]/text()").get()
        if furnished:
            furnished = furnished.split('meuble: "')[1].split('"')[0].strip()
            if furnished != '0':
                item_loader.add_value("furnished", True)

        floor = response.xpath("//script[contains(.,'latitude')]/text()").get()
        if floor:
            floor = floor.split('etage: "')[1].split('"')[0].strip()
            if floor != '0':
                item_loader.add_value("floor", floor)

        parking = response.xpath("//script[contains(.,'latitude')]/text()").get()
        if parking:
            parking = parking.split('nbParking: "')[1].split('"')[0].strip()
            if parking != '0':
                item_loader.add_value("parking", True)

        elevator = response.xpath("//script[contains(.,'latitude')]/text()").get()
        if elevator:
            elevator = elevator.split('ascenseur: "')[1].split('"')[0].strip()
            if elevator != '0':
                item_loader.add_value("elevator", True)

        balcony = response.xpath("//script[contains(.,'latitude')]/text()").get()
        if balcony:
            balcony = balcony.split('nbBalcons: "')[1].split('"')[0].strip()
            if balcony != '0':
                item_loader.add_value("balcony", True)

        terrace = response.xpath("//script[contains(.,'latitude')]/text()").get()
        if terrace:
            terrace = terrace.split('nbTerrasses: "')[1].split('"')[0].strip()
            if terrace != '0':
                item_loader.add_value("terrace", True)

        swimming_pool = response.xpath("//script[contains(.,'latitude')]/text()").get()
        if swimming_pool:
            swimming_pool = swimming_pool.split('piscine: "')[1].split('"')[0].strip()
            if swimming_pool != '0':
                item_loader.add_value("swimming_pool", True)

        item_loader.add_value("landlord_name", "Orpi YS Immobilier")
        item_loader.add_value("landlord_email", "ys-immobilier@orpi.com")
        item_loader.add_value("landlord_phone","02.35.42.41.02")


        yield item_loader.load_item()

class HTMLFilter(HTMLParser):
    text = ''
    def handle_data(self, data):
        self.text += data