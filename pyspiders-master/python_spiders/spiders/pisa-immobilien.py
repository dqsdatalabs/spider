# -*- coding: utf-8 -*-
# Author: Ahmed Omran
import scrapy
from ..loaders import ListingLoader
import requests
import re
from ..helper import *


class WellgroundedrealestateSpider(scrapy.Spider):
    name = "pisaimmobilien"
    start_urls = ['https://www.pisa-immobilien.de/immobilien-nutzungsart/wohnen/?post_type=immomakler_object&vermarktungsart&typ&ort&center&radius=25&objekt-id&collapse&von-qm=0.00&bis-qm=195.00&von-zimmer=0.00&bis-zimmer=5.00&von-kaltmiete=0.00&bis-kaltmiete=2200.00&von-kaufpreis=0.00&bis-kaufpreis=150000.00']
    country = 'germany'  # Fill in the Country's name
    locale = 'de'  # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type = 'testing'
    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url=url, method="GET", callback=self.parse)

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        ur= response.xpath('//div[@class="property-actions"]/div/a/@href').extract()
        urls=[ur[i] for i in range(len(ur)) if len(ur[i]) > 1]
        for x in range(len(urls)):
            url = urls[x]
            yield scrapy.Request(url=url, callback=self.populate_item)
        page=response.xpath('//div[@class="paginator row"]/div[@class="pages-nav col-xs-12 col-sm-7"]/span/a/@href').extract()[0]
        yield scrapy.Request(url=page, callback=self.parse)



    # 3. SCRAPING level 3
    def populate_item(self, response):
        room_count = None
        bathroom_count = None
        floor = None
        parking = None
        elevator = None
        balcony = None
        washing_machine = None
        dishwasher = None
        utilities = None
        terrace = None
        furnished = None
        property_type = None
        energy_label = None
        deposit = None
        square_meters=None
        available=None
        item_loader = ListingLoader(response=response)
        title = "".join(response.xpath('.//h1[@class="av-special-heading-tag "]/text()').extract()).strip()
        l=response.xpath('//div[@class="row price"]/div/text()').extract()
        d = [(l[i].replace("  ",""), l[i + 1].replace("  ","")) for i in range(0, len(l), 2)]
        ext = dict(d)
        l2=response.xpath('//ul[@class="list-group"]/li/div/div/text()').extract()
        try:
            rent = int(float(ext.get("Kaltmiete").replace(",00 EUR","").replace(",",".")))
        except:
            return
        external_id=l2[l2.index('Objekt ID')+1]

        try:
            bathroom_count = int(float(l2[l2.index('Badezimmer')+1].replace(",",".")))
        except:
            pass
        try:
            square_meters = int(float(l2[l2.index('Wohnfläche\xa0ca.')+1].replace(",",".").replace(" m²","")))
        except:
            pass
        try:
            deposit =  int("".join(re.findall(r'\b\d+\b',ext.get("Kaution")))) * rent
        except:
            pass
        try:
            room_count = int(float(l2[l2.index('Zimmer')+1].replace(",",".")))
        except:
            return

        try :
            if l2.index("Balkone"):
                balcony = True
        except :
            pass
        try:
            if l2.index("Ausstattung"):
                furnished = True
        except:
            pass
        try :
            floor=l2[l2.index('Etage')+1]
        except:
            pass
        try :
            heating_cost = int(float(ext.get("Heizkosten netto").replace(",00 EUR","").replace(",",".").replace(" EUR","")))
        except:
            heating_cost = int(float(ext.get("Warmmiete").replace(",00 EUR", "").replace(",", "").replace(" (Heizkosten enthalten)",""))) - rent

        utilities=int(float(ext.get("Betriebskosten brutto").replace(",00 EUR","").replace(",","")))
        try :
            available=l2[l2.index('Verfügbar ab')+1]
        except:
            pass
        extras = "".join(response.xpath('//div[@class="panel-body"]/ul/li/text()').extract())
        for j in range(len(extras)):
            if "nebenkosten" in extras[j].lower():
                utilities = int(extras[j].replace("Nebenkosten : ", "").replace(" €", ""))
            if 'kaution' in extras[j].lower():
                deposit = int(extras[j].replace("Kaution : ", "").replace(" €", ""))
        description = "".join(response.xpath('//div[@class="panel-body"]/p/text()').extract())
        description=description_cleaner(description)
        description=description.replace("www.pisa-immobilien.de"," ").replace("https://www.facebook.com/pisaimmobilienmanagement/","").replace("https:///pisaimmobilienmanagement","")
        pets_allowed, furnished, parking, elevator, balcony, terrace, swimming_pool, washing_machine, dishwasher=get_amenities(description,extras,item_loader)
        images = response.xpath('//div[@id="immomakler-galleria"]/a/@href').extract()
        landlord_name = response.xpath('//*[@id="av_section_2"]/div/div/div/div/div/div[2]/div[1]/div[2]/div[2]/div/div[2]/ul/li[1]/div/div[2]/span/text()').extract()
        if len(landlord_name)==0:
            landlord_name="IMMOBILIENMANAGEMENT GmbH & Co. KG"
        landlord_number = "0341 - 91 35 80"
        landlord_email = "info@pisa-immobilien.de"
        try:
            energy_label = l2[l2.index('Energie\xadeffizienz\xadklasse')+1]
        except:
            pass
        address = "".join(response.xpath('//p[@class="immomakler"]/text()').extract()).split(",")[0]
        pro="".join(response.xpath('//p[@class="immomakler"]/text()').extract()).split(",")[1]
        if "wohnung" in pro.lower() :
            property_type="apartment"
        else:
            property_type="house"
        longitude, latitude = extract_location_from_address(address)
        zipcode, city, addres = extract_location_from_coordinates(longitude, latitude)
        # # MetaData
        item_loader.add_value("external_link", response.url)  # String
        item_loader.add_value("external_source", self.external_source)  # String
        item_loader.add_value("external_id", external_id)  # String
        item_loader.add_value("position", self.position)  # Int
        item_loader.add_value("title", title)  # String
        item_loader.add_value("description", description)  # String
        # # Property Details
        item_loader.add_value("city", city)  # String
        item_loader.add_value("zipcode", zipcode)  # String
        item_loader.add_value("address", address)  # String
        item_loader.add_value("latitude", str(latitude))  # String
        item_loader.add_value("longitude", str(longitude))  # String
        item_loader.add_value("floor", floor)  # String
        item_loader.add_value("property_type", property_type)  # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters)  # Int
        item_loader.add_value("room_count", room_count)  # Int
        item_loader.add_value("bathroom_count", bathroom_count)  # Int

        item_loader.add_value("available_date", available)  # String => date_format

        item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking)  # Boolean
        item_loader.add_value("elevator", elevator)  # Boolean
        item_loader.add_value("balcony", balcony)  # Boolean
        item_loader.add_value("terrace", terrace)  # Boolean
        item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        item_loader.add_value("washing_machine", washing_machine)  # Boolean
        item_loader.add_value("dishwasher", dishwasher)  # Boolean

        # # Images
        item_loader.add_value("images", images)  # Array
        item_loader.add_value("external_images_count", len(images))  # Int
        # item_loader.add_value("floor_plan_images", floor_plan_images)  # Array

        # # Monetary Status
        item_loader.add_value("rent", int(rent))  # Int
        item_loader.add_value("deposit", deposit) # Int
        # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        item_loader.add_value("utilities", utilities)  # Int
        item_loader.add_value("currency", "EUR")  # String
        #
        # item_loader.add_value("water_cost", water_cost) # Int
        item_loader.add_value("heating_cost", heating_cost) # Int

        item_loader.add_value("energy_label", energy_label)  # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name)  # String
        item_loader.add_value("landlord_phone", landlord_number)  # String
        item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()
