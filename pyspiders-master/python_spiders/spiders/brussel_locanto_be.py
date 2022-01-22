# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

# from tkinter.font import ROMAN
from typing import NewType
from parsel.utils import extract_regex
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags 
from python_spiders.loaders import ListingLoader
import json
import dateparser
import re

class MySpider(Spider):
    name = 'brussel_locanto_be'
    execution_type='testing'
    country='belgium'
    locale='nl'
    external_source='Brussellocanto_PySpider_belgium'
    custom_settings = {
    "HTTPCACHE_ENABLED": False
    }
    def start_requests(self):
        start_urls = [
            {"url": "https://brussel.locanto.be/appartementen-te-huur/301/"}
        ]  # LEVEL 1       
        for url in start_urls:
            yield Request(url=url.get('url'),callback=self.parse,)
    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//a[@class='bp_ad__link']/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("property_type","apartment")
        title=response.xpath("//div[@class='vap_posting_header']/span/text()").get()
        if title:
            item_loader.add_value("title",title)
        external_id=response.xpath("//div[@class='vap_ad_id']/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id.split(":")[-1].strip())
        item_loader.add_value("address","brussel")
        rent=response.xpath("//div[@class='h1gray']/div/following-sibling::text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("€")[-1].split("(")[0].split(",")[0].strip())
        item_loader.add_value("currency","EUR")
        description="".join(response.xpath("//div[@id='js-user_content']/text() | //div[.='Overzicht']/following-sibling::div//text()").getall())
        if description:
            item_loader.add_value("description",description)
        room_count=response.xpath("//strong[.='Aantal kamers']/following-sibling::br/following-sibling::text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)   
        roomc=item_loader.get_output_value("room_count")
        if not roomc:
            room=response.xpath("//div[@class='vap_headline h2 ']/div/span/text()").get()
            if room:
                item_loader.add_value("room_count",room.split(",")[0].split("k")[0].strip())
            if "studio" in room:
                item_loader.add_value("room_count","1")
        square_meters=response.xpath("//strong[.='Woonoppervlakte']/following-sibling::br/following-sibling::text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.strip().split(" ")[0]) 
        images=[x for x in response.xpath("//a[@class='tn_img js-tn_img']/img/@src | //img[@id='big_img']/@src").getall()]
        if images:
            item_loader.add_value("images",images)
        latitude=response.xpath("//span[@itemprop='latitude']/text()").get()
        if latitude:
            item_loader.add_value("latitude",latitude)
        longitude=response.xpath("//span[@itemprop='longitude']/text()").get()
        if longitude:
            item_loader.add_value("longitude",longitude)
        item_loader.add_value("city","Brussels")
        item_loader.add_value("landlord_name","Locanto 15")

        yield item_loader.load_item()