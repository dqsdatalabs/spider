# -*- coding: utf-8 -*-
# Author: Ahmed Omran
import scrapy
from ..loaders import ListingLoader
import requests
import re
from ..helper import *


class hermannimmobilien(scrapy.Spider):
    name = "hermannimmobilien"
    start_urls = ['https://www.hermann-immobilien.de/immobilien-vermarktungsart/miete/?post_type=immomakler_object&center&radius=25&objekt-id&collapse&von-qm=0.00&bis-qm=730.00&von-zimmer=0.00&bis-zimmer=19.00&von-kaltmiete=0.00&bis-kaltmiete=2000.00&von-kaufpreis=0.00&bis-kaufpreis=2800000.00']
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
        ur= response.xpath('//div[@class="property"]/div/a/@href').extract()
        urls=[ur[i] for i in range(len(ur)) if len(ur[i]) > 1]
        for x in range(len(urls)):
            url = urls[x]
            yield scrapy.Request(url=url, callback=self.populate_item)
        page=response.xpath('//div[@class="pages-nav col-xs-12 col-sm-7"]/span/a/text()').extract()
        page=dict(zip(page,page))
        try :
            del page['« Zurück']
        except:
            pass
        try :
            del page["Weiter »"]
        except:
            pass
        m=int(max(page.values()))
        for i in range(2,m):
            urlpage=f"https://www.hermann-immobilien.de/immobilien-vermarktungsart/miete/page/{i}/?post_type=immomakler_object&center&radius=25&objekt-id&collapse&von-qm=0.00&bis-qm=730.00&von-zimmer=0.00&bis-zimmer=19.00&von-kaltmiete=0.00&bis-kaltmiete=2000.00&von-kaufpreis=0.00&bis-kaufpreis=2800000.00"
            yield scrapy.Request(url=urlpage, callback=self.parse)

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
        heating_cost=None
        item_loader = ListingLoader(response=response)
        title = "".join(response.xpath('.//div[@class="col-xs-12"]/h1/text()').extract()).strip()
        l=response.xpath('//div[@class="row price"]/div/text()').extract()

        l2=response.xpath('//div[@class="property-details"]/table/tr/td/text()').extract()
        try:
            rent =int(float(l2[l2.index('Kaltmiete:')+1].replace(" EUR","").replace(".","")))
        except:
            return
        external_id=l2[l2.index('Objekt ID:')+1]

        try:
            bathroom_count = int(float(l2[l2.index('Badezimmer:')+1].replace(",",".")))
        except:
            pass
        try:
            square_meters = int(float(l2[l2.index('Wohnfläche:')+1].replace(",",".").replace(" m²","")))
        except:
            pass
        try:
            deposit =  int(float(l2[l2.index('Kaution:')+1].replace(" EUR","").replace(".","")))
        except:
            pass
        try:
            room_count = int(float(l2[l2.index('Zimmeranzahl:')+1].replace(",",".")))
        except:
            pass
        try:
            heating_cost = int(float(l2[l2.index('Warmmiete:')+1].replace(" EUR","").replace(".",""))) -rent
        except:
            pass
        try:
            if "Ausstattung" in response.xpath('//div[@class="property-description"]/h3/text()').extract() :
                furnished = True
        except:
            pass
        try :
            floor=l2[l2.index('Etage:')+1]
        except:
            pass
        try:
            utilities = int(float(l2[l2.index('Nebenkosten:') + 1].replace(" EUR", "").replace(".", "")))
        except:
            pass

        try :
            available=l2[l2.index('Verfügbar ab:')+1]
        except:
            pass
        extras = "".join(response.xpath('//div[@class="property-features"]/ul/li/text()').extract())
        for j in range(len(extras)):
            if "nebenkosten" in extras[j].lower():
                utilities = int(extras[j].replace("Nebenkosten : ", "").replace(" €", ""))
            if 'kaution' in extras[j].lower():
                deposit = int(extras[j].replace("Kaution : ", "").replace(" €", ""))
        description = "".join(response.xpath('//div[@class="property-description"]/span/p/text()').extract())
        description=description_cleaner(description)
        pets_allowed, furnished, parking, elevator, balcony, terrace, swimming_pool, washing_machine, dishwasher=get_amenities(description,description,item_loader)
        images = response.xpath('//div[@class="owl-carousel__item__container"]/a/@href').extract()
        landlord_name="Hermann Immobilien GmbH"
        landlord_number = "+49 6181 9780-0"
        landlord_email = "vermietung@hermann-immobilien.de"
        try:
            en = response.xpath('//ul[@class="list-group"]/li/text()').extract()
            energy_label = "".join([x.split(":")[1] for x in en if "Energie" in x  ])
        except:
            pass
        address = "".join(response.xpath('//div[@class="col-xs-12"]/h2/text()').extract()).split(",")[0]
        pro="".join(response.xpath('//div[@class="col-xs-12"]/h2/text()').extract()).split(",")[1]
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
