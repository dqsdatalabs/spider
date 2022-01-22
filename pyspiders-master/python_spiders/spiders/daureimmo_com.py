# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re
class MySpider(Spider):
    name = 'daureimmo_com'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.daureimmo.com/ajax/ListeBien.php?menuSave=5&page=1&ListeViewBienForm=text&vlc=4&filtre=2&lieu-alentour=0&langue=fr&MapWidth=100&MapHeight=650&DataConfig=JsConfig.GGMap.Liste&Pagination=0",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.daureimmo.com/ajax/ListeBien.php?menuSave=5&page=1&ListeViewBienForm=text&vlc=4&filtre=8&lieu-alentour=0&langue=fr&MapWidth=100&MapHeight=650&DataConfig=JsConfig.GGMap.Liste&Pagination=0",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//a[@itemprop='url']/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        next_button = response.xpath("//span[@class='PageSui']/a/@href").get()
        if next_button:
            yield Request(response.urljoin(next_button), callback=self.parse, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])
        item_loader.add_value("external_source", "Daureimmo_PySpider_france")     
        title = " ".join(response.xpath("//h1[@class='detail-titre']//text()[.!=' 0']").getall())
        if title:
            item_loader.add_value("title", re.sub("\s{2,}", " ", title)) 
    
        item_loader.add_xpath("external_id", "//div[span[.='Ref']][1]//span[@itemprop='productID']/text()")
        room_count = response.xpath("//li[span[@class='ico-chambre']]/text()[not(contains(.,'NC'))]").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split("chambre")[0])
        else:
            room_count = response.xpath("//li[span[@class='ico-piece']]/text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count.split("pièce")[0])

        address = response.xpath("//h2[@class='detail-bien-ville']/text()").get()
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("city", address.split("(")[0].strip())
            item_loader.add_value("zipcode", address.split("(")[-1].split(")")[0].strip())
      
        item_loader.add_xpath("latitude", "//li[@class='gg-map-marker-lat']/text()")
        item_loader.add_xpath("longitude", "//li[@class='gg-map-marker-lng']/text()")
        square_meters = response.xpath("//li[span[@class='ico-surface']]/text()[not(contains(.,'NC'))]").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("m")[0])
      
        description = " ".join(response.xpath("//div[contains(@class,'detail-bien-desc-content ')]/p//text()").getall()) 
        if description:
            item_loader.add_value("description", description.strip())

        images = [x for x in response.xpath("//div[@class='big-flap-container']//div[@class='diapo is-flap']/img/@data-src").getall()]
        if images:
            item_loader.add_value("images", images)
        rent =" ".join(response.xpath("//div[contains(@class,'detail-bien-prix')]/text()").getall())
        if rent:
            item_loader.add_value("rent_string", rent.strip().replace(" ",""))
        utilities = response.xpath("//li/i[span[contains(.,'charges')]]/span[@class='cout_charges_mens']/text()[.!='0']").get()
        if utilities:
            item_loader.add_value("utilities", utilities)
        deposit = response.xpath("//li[span[contains(.,'Dépôt de garantie')]]/span[@class='cout_honoraires_loc']/text()[.!='0']").get()
        if deposit:
            item_loader.add_value("deposit", deposit.replace(" ",""))
        landlord_name = response.xpath("//div[contains(@class,'contact-agence-agent-locsaison')]/div[@class='heading3']/text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name.strip())
        landlord_phone = response.xpath("//div[contains(@class,'contact-agence-agent-locsaison')]//span[.='Tel.']/following-sibling::text()[1]").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone.strip())
    
        yield item_loader.load_item()