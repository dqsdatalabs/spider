# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import math
from  geopy.geocoders import Nominatim
from html.parser import HTMLParser 
import re

class MySpider(Spider):
    name = 'annemano_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    custom_settings = {
        "HTTPCACHE_ENABLED": False,
        "HTTPERROR_ALLOWED_CODES": [404]
    }

    def start_requests(self):
        start_urls = [
            {"url": "https://www.anne-mano-immobilier.fr/immobilier-annonces/?location_text=&geo_lat=&geo_long=&geo_radius=3&cat=louer&types=maison&min_bedrooms=0&max_bedrooms=14&min_amountrooms=0&max_amountrooms=20&min_price=0&max_price=1500000&min_area=0&max_area=600", "property_type": "house"},
            {"url": "https://www.anne-mano-immobilier.fr/immobilier-annonces/?location_text=&geo_lat=&geo_long=&geo_radius=3&cat=louer&types=appartement&min_bedrooms=0&max_bedrooms=14&min_amountrooms=0&max_amountrooms=20&min_price=0&max_price=1500000&min_area=0&max_area=600", "property_type": "apartment"}
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//meta[@itemprop='url']/@content").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
            
# 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_xpath("title", "//h2[@class='entry-title']/text()") # 
        item_loader.add_value("property_type", response.meta.get('property_type'))

        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", "Annemano_PySpider_"+ self.country + "_" + self.locale)

        address = response.xpath("//div[@class='elementor-custom-embed']/iframe/@title").get()
        if address:
            item_loader.add_value("address", address)
                
        # latitude = response.xpath("//div[@id='googleMap_shortcode']/@data-cur_lat").get()
        # longitude = response.xpath("//div[@id='googleMap_shortcode']/@data-cur_long").get()
        # if latitude and longitude:
        #     latitude = latitude.strip()
        #     longitude = longitude.strip()
        #     item_loader.add_value("longitude", longitude)
        #     item_loader.add_value("latitude", latitude)

        square_meters = response.xpath("//li[@class='property-label-areasize']/div/span[1]/text()").get()
        if square_meters:            
            item_loader.add_value("square_meters", square_meters.strip())

        room_count = response.xpath("//li[@class='property-label-bedrooms']/div/span[1]/text()").get()
        if room_count:        
            item_loader.add_value("room_count", room_count.strip())

        details = "".join(d for d in response.xpath("//div[@class='property-label']//text()").getall() if d != '\n')
        bath = re.search(r"d'eau:(\d)", re.sub(r'\s', '', details))
        if bath:
            item_loader.add_value('bathroom_count', bath.group(1))
        
        rent ="".join(response.xpath("//div[@class='property-label']/div//text()").getall())
        if rent:
            rent = rent.replace('\n', '').split("Prix")[-1].strip()
            rent=re.findall("\d+",rent)
            item_loader.add_value("rent", rent[0])
        item_loader.add_value("currency", 'EUR')

        external_id = response.xpath("//strong[contains(.,'REF')]/text()").re_first(r'\d+')
        if external_id:
            item_loader.add_value("external_id", external_id)

        description = "".join(response.xpath("//h3/following-sibling::p[2]/text()").getall())  
        if description:
            item_loader.add_value("description", description)

        zipcode = response.xpath("//div[contains(., 'Postal')]//text()").re_first(r'\d{5}')
        if zipcode:
            item_loader.add_value('zipcode', zipcode)
        
        images = [x for x in response.xpath("//div[@class='swiper-wrapper opalestate-gallery']//div/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))

        energy_label = response.xpath("//span[@class='diagnostic-number']/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label)

        landlord_name = response.xpath("//h4/a/text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name.strip())

        landlord_phone = response.xpath("//a[contains(@href, 'tel:')]/text()").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone.strip())
        
        landlord_email = response.xpath("//a[contains(@href, 'mailto')]/span/text()").get()
        if landlord_email:
            item_loader.add_value('landlord_email', landlord_email)

        yield item_loader.load_item()

class HTMLFilter(HTMLParser):
    text = ''
    def handle_data(self, data):
        self.text += data