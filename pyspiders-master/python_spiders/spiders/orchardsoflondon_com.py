# Author: Madhumitha S
# Team: Sabertooth

from datetime import datetime
from math import ceil

import re


import js2xml
import lxml.etree
import scrapy
from geopy.geocoders import Nominatim
from scrapy import Selector

from ..helper import extract_number_only, remove_unicode_char
from ..loaders import ListingLoader
from ..user_agents import random_user_agent


class OrchardsoflondonSpider(scrapy.Spider):
    name = 'orchardsoflondon_com'
    allowed_domains = ['www.orchardsoflondon.com']
    start_urls = ['https://www.orchardsoflondon.com/']
    execution_type = 'testing'
    country = 'united_kingdom'
    external_source = "Orchardsoflondon_PySpider_united_kingdom_en"
    locale = 'en'
    custom_settings = {"HTTPCACHE_ENABLED":False}
    thousand_separator = ','
    scale_separator = '.'
    position = 0

    def start_requests(self):
        start_url = [
            {   'url' : "https://www.gibbs-gillespie.co.uk/property-search/residential-houses-available-to-rent-in-south-east-england",
                'property_type' : 'house'},
            # {
            #     'url' : 'https://www.gibbs-gillespie.co.uk/property-search/residential-bungalows-available-to-rent-in-south-east-england',
            #     'property_type' : 'house'
            # },
            {
                'url' : 'https://www.gibbs-gillespie.co.uk/property-search/residential-apartments-available-to-rent-in-south-east-england',
                'property_type' : 'apartment'
            }]

        for url in start_url:
            yield scrapy.Request(url=url['url'],
                                 callback=self.parse,
                                 meta={'request_url': url['url'],
                                 'property_type' : url['property_type']})

    def parse(self, response, **kwargs):
        seen = False
        urls = response.xpath("//div[@class='property-info']//address/a/@href").getall()
        for url in urls:
            seen = True
            follow_url = "https://www.gibbs-gillespie.co.uk/" + url

            yield scrapy.Request(
                dont_filter=True,
                url=follow_url,
                callback=self.get_property_details,
                meta={
                      'property_type' : response.meta.get('property_type')})

        next_page_url = response.xpath("//a[@title='Next']/@href").get()

        # with open("xxx.urls","a",encoding='utf-8') as file:
        #     file.write(url+"\n")
        if next_page_url and seen:

            yield scrapy.Request(
                dont_filter=True,
                url= "https://www.gibbs-gillespie.co.uk/" + next_page_url,

                callback=self.parse,
                meta = {'property_type' : response.meta.get('property_type')})


    def get_property_details(self, response):

        item_loader = ListingLoader(response=response)
        item_loader.add_value('external_link', response.url)

        pictures = response.xpath("//div[@class='slider']/div/@style").extract()
        if pictures:
            for image in pictures:
                item_loader.add_value('images', image.split("(")[1].split(")")[0])
        else:
            item_loader.add_xpath('images', '//img[@class="img-responsive"]/@src')

        item_loader.add_xpath("floor_plan_images", "//div[@id='tab7']/a/img/@src")
        item_loader.add_xpath('rent_string', "//span[@class='price-qualifier']/text()")
        # item_loader.add_xpath('room_count', "//li[contains(@class,'Bedroom')]/text()")
        item_loader.add_xpath('bathroom_count', "//li[contains(@class,'Bathroom')]/text()")
        item_loader.add_xpath("title", ".//h1/text()")
        description = response.xpath(".//p[@class='introtext']/text()").extract_first()
        if description:
            item_loader.add_value('description', remove_unicode_char(description))

        address = response.xpath("//h1[@class='house-title']/strong/text()").get()
        if address:
            item_loader.add_value("address",address)
        #Setting city as London sice all the properties in the website are in London
        item_loader.add_value('city', 'London')

        title = response.xpath("//h1[@class='house-title']/strong/text()").get()
        if title:
            item_loader.add_value("title",title)
            room_count = title.split()[0]
            item_loader.add_value("room_count",room_count)
        # zipcode = re.findall(r'\w+\d+',item_loader.get_output_value('address'))
        # if zipcode:
        #     item_loader.add_value('zipcode', zipcode[0])

        # Square_meters check
        sqm_check = response.xpath("//li[contains(@class,'SquareFeet')]")
        if sqm_check:
            square_feet = extract_number_only(response.xpath("//li[contains(@class,'SquareFeet')]/@class").extract_first())
            square_meters = str((ceil(float(square_feet))))
            item_loader.add_value('square_meters', square_meters)

        javascript = response.xpath('.//*[contains(text(),"lng")]/text()').extract_first()
        if javascript:
            xml = lxml.etree.tostring(js2xml.parse(javascript), encoding='unicode')
            selector = Selector(text=xml)
            latitude = selector.xpath('.//property[@name="lat"]/number/@value').get()
            longitude = selector.xpath('.//property[@name="lng"]/number/@value').get()
            item_loader.add_value('latitude', latitude)
            item_loader.add_value('longitude', longitude)

            """
            geolocator = Nominatim(user_agent=random_user_agent())
            location = geolocator.reverse(f"{latitude}, {longitude}")
            if location and 'address' in location.raw:
                if 'postcode' in location.raw['address']:
                    item_loader.add_value('zipcode', location.raw['address']['postcode'])
                if 'city' in location.raw['address']:
                    item_loader.add_value('city', location.raw['address']['city'])
            """

        item_loader.add_value('landlord_name', 'Orchards of London')
        item_loader.add_value('landlord_phone', "020 3962 6000")

        features = " ".join(response.xpath("//ul[@class='attributes']/li//text()").extract())
        # Available date check
        available_now = response.xpath(".//li[contains(@class,'Availablenow')]")
        if available_now:
            item_loader.add_value('available_date', datetime.today().strftime('%Y-%m-%d'))
        else:
            available_check = re.findall(r'available (\d+)\w+ (\w+)',features.lower())
            if available_check:
                now = datetime.now()
                if now.year == 2020:
                    available_date = available_check[0][0]+ " " + available_check[0][1].capitalize() + " " + str(now.year+1)
                else:
                    available_date = available_check[0][0]+ " " + available_check[0][1].capitalize() + " " + str(now.year)
                item_loader.add_value('available_date',datetime.strptime(available_date, "%d %B %Y").strftime('%Y-%m-%d')) 

        item_loader.add_value('property_type', response.meta.get('property_type'))

        # "https://www.orchardsoflondon.com//property-to-rent/property-to-rent/lowfield-road-acton-london-w3/4775"
        if "parking" in features.lower() or 'garage' in features.lower():
            item_loader.add_value('parking', True)

        # "https://www.orchardsoflondon.com//property-to-rent/property-to-rent/bromyard-avenue-acton-w3/4730"
        if "terrace" in features.lower():
            item_loader.add_value('terrace', True)

        # https://www.orchardsoflondon.com/property-to-rent/house-to-rent/west-road-ealing-w5/4955
        if "swimming" in features.lower() or "pool" in features.lower():
            item_loader.add_value('swimming_pool', True)

        # https://www.orchardsoflondon.com/property-to-rent/flat-to-rent/castlebar-road-ealing-w5/4934
        if " furnished" in features.lower() and "unfurnished" not in features.lower():
            item_loader.add_value('furnished', True)

        # https://www.orchardsoflondon.com/property-to-rent/property-to-rent/longfield-avenue-london-w5/4682
        if "elevator" in features.lower() or "lift" in features.lower():
            item_loader.add_value('elevator', True)

        self.position += 1
        item_loader.add_value('position', self.position)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("currency","GBP")
        yield item_loader.load_item()

