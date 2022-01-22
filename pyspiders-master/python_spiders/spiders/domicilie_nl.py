# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from python_spiders.loaders import ListingLoader
from scrapy.loader.processors import MapCompose
from scrapy.spiders import Spider
from scrapy import Request
from python_spiders.loaders import ListingItem
from w3lib.html import remove_tags
# from crawler.base import BaseSpider 
import dateparser
import re

class MySpider(Spider): 
    name = "domicilie_nl"
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'                                                                                                                                                                                                                                                 
    external_source = "Domicilie_PySpider_netherlands_nl"
    def start_requests(self):
        start_urls = [
            {
                "url" : "https://domicilie.nl/wonen/aanbod/huur/?search_total=&minhuurprijs=0&maxhuurprijs=6000&woontype=appartement",
                "property_type" : "apartment"
            }, 
            {
                "url" : "https://domicilie.nl/wonen/aanbod/huur/?search_total=&minhuurprijs=0&maxhuurprijs=6000&woontype=",
                "property_type" : "house" 
                 
            },
            # {
            #     "url" : "https://domicilie.nl/huurwoningen?combine=&field_type_woning_value=3&field_aantal_kamers_value=All&field_woonoppevlakte_value=All&field_huurprijs_value=1&field_huurprijs_value_1=11",
            #     "property_type" : "house"
                
            # },
            # {
            #     "url" : "https://domicilie.nl/huurwoningen?combine=&field_type_woning_value=4&field_aantal_kamers_value=All&field_woonoppevlakte_value=All&field_huurprijs_value=1&field_huurprijs_value_1=11",
            #     "property_type" : "house"
                
            # },
            # {
            #     "url" : "https://domicilie.nl/huurwoningen?combine=&field_type_woning_value=5&field_aantal_kamers_value=All&field_woonoppevlakte_value=All&field_huurprijs_value=1&field_huurprijs_value_1=11",
            #     "property_type" : "house"
                
            # },
            # {
            #     "url" : "https://domicilie.nl/huurwoningen?combine=&field_type_woning_value=6&field_aantal_kamers_value=All&field_woonoppevlakte_value=All&field_huurprijs_value=1&field_huurprijs_value_1=11",
            #     "property_type" : "house"
                
            # },
            # {
            #     "url" : "https://domicilie.nl/huurwoningen?combine=&field_type_woning_value=7&field_aantal_kamers_value=All&field_woonoppevlakte_value=All&field_huurprijs_value=1&field_huurprijs_value_1=11",
            #     "property_type" : "house"
                
            # },
        ]# FIRST LEVEL
        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        # url(href attribute) which leads to the(SECOND LEVEL) page where data extraction is needed
        for follow_url in response.xpath("//div[@class='item']/a/@href").extract():
            follow_url = response.urljoin(follow_url)
            # request the url and callback the function that extracts data
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})

        # then yield the function which paginates to another page that contains data
        yield from self.paginate(response)

    # 2. SCRAPING LEVEL 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source" , self.external_source)
        title=response.xpath("//h3/span/text()").get()
        if title:
           item_loader.add_value("title",title)

        item_loader.add_value("external_link", response.url)
        
        desc = "".join(response.xpath("//p[contains(text(),'Indeling:')]/br/following-sibling::text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())
        # latLng = response.xpath("//script[contains(.,'google.maps')]/text()").get()
        # if latLng:
        #     latitude = latLng.split("LatLng(")[1].split("),")[0].split(",")[0].strip()
        #     longitude = latLng.split("LatLng(")[1].split("),")[0].split(",")[1].strip()
        #     if latitude and longitude:
        #         item_loader.add_value("latitude", latitude)
        #         item_loader.add_value("longitude", longitude)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        
        square_meters =response.xpath("//p[contains(.,' Woonoppervlakte')]/following-sibling::p/text()").get()
        if square_meters:
            square_meters =re.findall("\d+",square_meters)
        item_loader.add_value("square_meters", square_meters) 
        
        room_count ="".join(response.xpath("//div[@class='tekst hide']//p//text()").getall())
        if room_count:
            index=room_count.find("slaapkamers")
            if index:
                room=re.findall("\d+",room_count[(index-5):])
                item_loader.add_value("room_count", room[0])

        # bathroom_count = response.xpath("//p[./span[contains(.,'badkamer')]]/text()").get()
        # if bathroom_count:
        #     bathroom_count = room_count.split(" ")[0]
        # item_loader.add_value("bathroom_count", bathroom_count)
        
        address = response.xpath("//h3/span/text()").get()
        if address:
            adres=address.strip() 
            city=adres.split("–")[-1].strip()
            city1=city.strip().split(" ")[0]
            zipcode=re.findall("\d+",city)
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", city1)
            item_loader.add_value("zipcode", zipcode)

        images = [x for x in response.xpath("//div[contains(@class,'gallery_image')]//a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
        item_loader.add_value("landlord_name", "Domicilie")
        item_loader.add_value("landlord_phone", "0318 69 35 02")
        item_loader.add_value("landlord_email", "ede@domicilie.nl ")

        price = response.xpath("//p[contains(.,'Vraagprijs')]/following-sibling::p/text()").get()
        if price:
            price=price.split(",")[0].replace(".","")
            price=re.findall("\d+",price)
            item_loader.add_value("rent", price)
            item_loader.add_value("currency", "EUR")

        yield item_loader.load_item()

    # 3. PAGINATION LEVEL 2
    def paginate(self, response):
        next_page_urls = response.xpath(
            "//div[@class='nav-links']//a[@class='next page-numbers']/@href"
        ).extract()  # pagination("next button") <a> element here
        for next_page_url in next_page_urls:
            yield response.follow(response.urljoin(next_page_url), self.parse, meta={'property_type': response.meta.get('property_type')})
            
