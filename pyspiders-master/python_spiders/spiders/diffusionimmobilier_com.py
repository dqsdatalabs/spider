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

class MySpider(Spider):
    name = 'diffusionimmobilier_com'
    execution_type='testing'
    country='france'
    locale='fr'
    
    def start_requests(self):
        start_urls = [
            {"url": "https://www.diffusion-immobilier.com/ajax/ListeBien.php?page=1&TypeModeListeForm=text&ope=2&filtre=2&lieu-alentour=0&langue=fr&MapWidth=100&MapHeight=0&DataConfig=JsConfig.GGMap.Liste&Pagination=0", "property_type": "apartment"},
            {"url": "https://www.diffusion-immobilier.com/ajax/ListeBien.php?page=1&TypeModeListeForm=text&ope=2&filtre=1&lieu-alentour=0&langue=fr&MapWidth=100&MapHeight=0&DataConfig=JsConfig.GGMap.Liste&Pagination=0", "property_type": "house"}
            
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse, meta={"property_type": url.get("property_type")})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='bloc-biens']/div[contains(@class,'liste-bien-container')]//a[contains(.,'Détails')]/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type' : response.meta.get("property_type")})
            
# 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        title = " ".join(response.xpath("//div[contains(@class,'detail-bien-title')]/h2//text()").extract())
        item_loader.add_value("title", title.strip())
        item_loader.add_value("property_type", response.meta.get('property_type'))

        item_loader.add_value("external_source", "Diffusionimmobilier_PySpider_" + self.country + "_" + self.locale)
        item_loader.add_value("external_link", response.url)
        
        external_id = response.xpath("//div[contains(@class,'detail-bien-ref')][2]/span[@itemprop='productID']/text()").extract_first()
        if external_id:
            item_loader.add_value("external_id", external_id.strip())

        price = response.xpath("//div[@class='detail-bien-prix hidden']//costpermonth/@data-price").extract_first()
        if price:
            item_loader.add_value("rent_string", price.replace(" ",""))
        item_loader.add_value("currency", "EUR")

        utilities = response.xpath("//div[@class='detail-bien-desc-content clearfix']/div/ul/li/span[contains(.,'Honoraires charge')]/following-sibling::span/text()[.!='0']").extract_first()
        if utilities:
            item_loader.add_value("utilities", int(float(utilities)))
 
        room_count = response.xpath("//ul/li/span[@class='ico-piece']/following-sibling::text()").extract_first()
        if room_count:
            item_loader.add_value("room_count", room_count.split("pièce")[0].strip())

        square = response.xpath("//ul/li/span[@class='ico-surface']/following-sibling::text()").extract_first()
        if square:
            square_meters = square.split("m")[0].strip()
            item_loader.add_value("square_meters", square_meters)
        
        desc = "".join(response.xpath("//span[@itemprop='description']/p/text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())

        address = response.xpath("//h2[@class='detail-bien-ville']//text()").extract_first()
        if address:     
            item_loader.add_value("city", address.split("(")[0].strip())
            item_loader.add_value("address", address.split("(")[0].strip())
            zipcode=address.split("(")[1].split(")")[0]
            if zipcode:
                item_loader.add_value("zipcode", zipcode)
  
        deposit = response.xpath("//ul/li/span[contains(.,'Dépôt de garantie')]/following-sibling::span/text()").extract_first()
        if deposit:
            item_loader.add_value("deposit", deposit.replace(" ","").strip())

        item_loader.add_xpath("latitude", "//div[@class='gg-map']//li[@class='gg-map-marker-lat']/text()")
        item_loader.add_xpath("longitude", "//div[@class='gg-map']//li[@class='gg-map-marker-lng']/text()")
   
        images = [response.urljoin(x) for x in response.xpath("//div[@class='is-flap scrollpane-to-child']//img[2]/@src").extract()]
        if images is not None:
            item_loader.add_value("images", images)      

        item_loader.add_value("landlord_email", "copro@diffusion-immobilier.com")
        item_loader.add_value("landlord_name", "diffusion immobilier")
        item_loader.add_value("landlord_phone", "04 92 81 15 96")
        yield item_loader.load_item()
