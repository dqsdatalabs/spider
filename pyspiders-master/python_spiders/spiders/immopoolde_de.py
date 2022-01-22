# -*- coding: utf-8 -*-
# Author: Abanoub Moris
import json
import scrapy
from urllib.parse import urlparse, urlunparse, parse_qs
from parsel import Selector
from scrapy.http.request import Request
from ..loaders import ListingLoader
from ..helper import *
import re


class ImmopoolSpider(scrapy.Spider):

    name = "immopool_de"
    country = 'germany'  # Fill in the Country's name
    locale = 'de'  # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'
    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        LASID=Selector(requests.get("https://www.immopool.de/ASP/Start.asp?PoolNr=2").text).css(".headTopL a::attr(href)").get()
        LASID = re.search(r'\d+',LASID)[0]
        for i in range(1,17):
            url = f'https://www.immopool.de/ASP/immo/obj/ImmoListe.asp?LASID={LASID}&GrpO=1&SL=&BEZ=H%E4user&AnbNr=&Firma=&PRArt=2&ORTArt=1&GeoSL=004&mitBild=on&Waehr=EUR&SrcAction4711={i}'
            yield Request(url, callback=self.parse)
        for i in range(1,44):
            url=f'https://www.immopool.de/ASP/immo/obj/ImmoListe.asp?LASID={LASID}&GrpO=2&SL=0302200,%200302700&BEZ=Wohnungen&AnbNr=&Firma=&PRArt=2&ORTArt=1&Land=D&GeoSL=004&Waehr=EUR&SrcAction4711={i}'
            yield Request(url, callback=self.parse)
    # 2. SCRAPING level 2

    def parse(self, response):
        apartments = response.css(".AltListHighlight a::attr(href)").getall()
        apartments=['https://www.immopool.de/ASP/immo/obj/'+x for x in apartments]
        for link in apartments:
            yield scrapy.Request(link, callback=self.populate_item)

    def populate_item(self, response):

        typ = "".join(response.css("h2.highl2 *::text").getall()).lower()
        if 'garage' in typ or 'stellflächen' in typ:
            return
        property_type = 'house' if 'doppelhaushälfte' in typ or 'haus' in typ or 'bungalow' in typ or 'häus' in typ else 'apartment'

        title = response.css(".expose h1::attr(title)").get().lower()
        if 'auto' in title or 'stellplatz' in title or 'garage' in title:
            return
        if 'Außenstellplatz'.lower() in title or 'Stellplätze'.lower() in title or 'park' in title:
            return
        rent = response.css(".eckdaten tr:contains(preis) .dataVal::text").get().replace('.','')
        if rent:
            rent = re.search(r'\d+',rent)[0]

        room_count = response.css(".eckdaten tr:contains(immer) .dataVal::text").get()
        if room_count:
            room_count=re.search(r'\d+',room_count)[0]
        else:
            room_count='1'
        if room_count=='0':
            room_count='1'
        available_date = response.css(".eckdaten tr:contains('Frei ab') .dataVal::text").get()
        if available_date:
            available_date = available_date.replace('Ab','')

        utilities = response.css(".eckdaten tr .dataBez:contains(ebenkosten) +::text").get()
        if utilities:
            try:
                utilities = re.search(r'\d+',utilities.replace('.',''))[0]
            except:
                pass
            

        square_meters=''
        try:
            square_meters = response.css(".eckdaten tr:contains('Wohnfläche') .dataVal::text")[1].get()
        except:
            pass
        
        if square_meters:
            square_meters=re.search(r'\d+',square_meters)[0]

        external_id = response.css(".eckdaten tr .onlinenr::text").get()
        description = "".join(response.css(".exposeLM .exposeBox::text").getall())
        rx = re.search(r'[\w]+.[\w]+@[\w]+.[\w]+.[\w]+',description)
        landlord_email=''
        if rx:
            landlord_email = rx[0]
        description = description_cleaner(description)

        images = response.css(".thumbs a::attr(href)").getall()
        if len(images)==0:
            return
        images = [x[:x.find('&width')] for x in images]

        ##################################**RETURN**###################################
        address = response.css("iframe::attr(src)").get()
        address = parse_qs(urlparse(address).query)["ort"][0]+', '+'Germany'
        longitude,latitude = extract_location_from_address(address)
        zipcode, city, addres = extract_location_from_coordinates(longitude,latitude)

        landlord_phone_lst = response.css(".dataAnb p::text,.box p::text").getall()
        landlord_phone=''
        if landlord_phone_lst:
            landlord_phone = landlord_phone_lst[-1]
            if not landlord_phone or not landlord_phone[1].isdigit():
                landlord_phone='057237918-6023'

        landlord_name=response.css(".dataAnb p a::text").get()
        if not landlord_name:
            landlord_name='Immopool'
       

        if int(rent) > 0 and int(rent) < 20000:
            item_loader = ListingLoader(response=response)

            # # MetaData
            item_loader.add_value("external_link", response.url)  # String
            item_loader.add_value(
                "external_source", self.external_source)  # String

            item_loader.add_value("external_id", str(external_id))  # String
            item_loader.add_value("position", self.position)  # Int
            item_loader.add_value("title", title)  # String
            item_loader.add_value("description", description)  # String

            # # Property Details
            item_loader.add_value("city", city)  # String
            item_loader.add_value("zipcode", zipcode)  # String
            item_loader.add_value("address", address)  # String
            item_loader.add_value("latitude", str(latitude))  # String
            item_loader.add_value("longitude", str(longitude))  # String
            # item_loader.add_value("floor", floor)  # String
            item_loader.add_value("property_type", property_type)  # String
            item_loader.add_value("square_meters", square_meters)  # Int
            item_loader.add_value("room_count", room_count)  # Int
            #item_loader.add_value("bathroom_count", bathroom_count)  # Int

            item_loader.add_value("available_date", available_date)

            get_amenities(description, '', item_loader)

            # # Images
            item_loader.add_value("images", images)  # Array
            item_loader.add_value(
                "external_images_count", len(images))  # Int
            # item_loader.add_value("floor_plan_images", floor_plan_images)  # Array

            # # Monetary Status
            item_loader.add_value("rent", rent)  # Int
            #item_loader.add_value("deposit", deposit)  # Int
            # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
            item_loader.add_value("utilities", utilities)  # Int
            item_loader.add_value("currency", "EUR")  # String

            # item_loader.add_value("water_cost", water_cost) # Int
            # item_loader.add_value("heating_cost", heating_cost) # Int
            # item_loader.add_value("energy_label", energy_label)  # String

            # # LandLord Details
            item_loader.add_value(
                "landlord_name", landlord_name)  # String
            item_loader.add_value(
                "landlord_phone", landlord_phone)  # String
            item_loader.add_value("landlord_email", landlord_email)  # String

            self.position += 1
            yield item_loader.load_item()
