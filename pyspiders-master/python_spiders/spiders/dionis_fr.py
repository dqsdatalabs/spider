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
import re

class MySpider(Spider):
    name = 'dionis_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    custom_settings = {
        "PROXY_ON": True,
        # "PROXY_FR_ON": True,
        "HTTPCACHE_ENABLED": False
    }

    def start_requests(self):
        start_urls = [
            {"url": "https://www.dionis.fr/fr/listing-location.html?loc=location&type%5B%5D=appartement&surfacemin=&prixmax=&numero=&coordonnees=&archivage_statut=0&tri=&page=1", "property_type": "apartment"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'),
                            })
    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//article"):
            follow_url = item.xpath("./a/@href").extract_first()
            lat = item.xpath("./@data-insee").extract_first().split("_")[0]
            lng = item.xpath("./@data-insee").extract_first().split("_")[1]

            yield Request(follow_url, callback=self.populate_item, meta={"lat": lat, "lng": lng, 'property_type': response.meta.get('property_type')})


    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Dionis_PySpider_"+ self.country + "_" + self.locale)

        lat = response.meta.get("lat")
        lng = response.meta.get("lng")
        prop_type = response.meta.get("property_type")
        
        title = " ".join(response.xpath("//h1/span//text()").extract())
        item_loader.add_value("title", title)
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("property_type", prop_type)

        desc = "".join(response.xpath("//div[@id='descdetail']/div/div[2]//text()").extract())
        item_loader.add_value("description", re.sub('\s{2,}', ' ', desc.strip()))

        price = "".join(response.xpath("//div[@id='detailinfosdetail']//li[@class='c_prix']//span[@class='champ']/text()").extract())
        if price:
            item_loader.add_value("rent", price.strip().replace("\xa0","."))
        item_loader.add_value("currency", "EUR")

        city = response.xpath("//div[@id='descdetail']/div//div[@class='info_ville']/text()").get()
        if city:
            item_loader.add_value("city", city.capitalize())

        utilities = response.xpath("//div[@class='info_prix-hai']//text()[contains(.,'Charges :')]").extract_first()
        if utilities:
            item_loader.add_value("utilities",utilities.split(":")[1].split("€")[0].strip())

            
        deposit = response.xpath("normalize-space(//div[@id='descdetail']//div[@class='info_prix-hai']/text()[contains(.,'Dépôt')])").get()
        if deposit:
            item_loader.add_value(
                "deposit", deposit.split(":")[1].strip().split("€")[0].replace("\xa0",""))
        
        
        item_loader.add_xpath(
            "external_id", "normalize-space(//div[@id='detailinfosdetail']//li[@class='c_numero']//span[@class='champ'])"
        )

        square = response.xpath(
            "normalize-space(//div[@id='detailinfosdetail']//li[@class='c_surface']//span[@class='champ']/text())"
        ).get()
        if square:
            item_loader.add_value(
                "square_meters", str(math.ceil(float(square.replace(",", "."))))
            )
        
        room_count = response.xpath(
            "normalize-space(//div[@id='detailinfosdetail']//li[@class='c_piece']//span[@class='champ'])"
        ).get()
        if room_count:
            item_loader.add_value("room_count", room_count)
            
        item_loader.add_xpath("floor", "normalize-space(//div[@id='detailinfosdetail']//li[@class='c_etages']//span[@class='champ'])")

        address = response.xpath("normalize-space(//div[@id='detailinfosdetail']//li[@class='c_ville']//span[@class='champ'])").get()
        item_loader.add_value("address", address)
            
       
        elevator = response.xpath(
            "normalize-space(//div[@id='detailinfosdetail']//li[@class='c_ascenseur']//span[@class='champ'])").get()
        if elevator:
            if "Oui" in elevator or "Yes" in elevator:
                item_loader.add_value("elevator", True)
            else:
                item_loader.add_value("elevator", False)
                
        images = [
            response.urljoin(x)
            for x in response.xpath(
                "//div[@id='sliderdetail']/ul/li/img/@src"
            ).extract()
        ]
        if images:
            item_loader.add_value("images", images)
        
        furnished = response.xpath("//h2[contains(@class,'info_titre')]//text()").get()
        if furnished and furnished.lower() in "meuble":
            item_loader.add_value("furnished", True)

        item_loader.add_value("landlord_phone", "01 46 36 24 42")
        item_loader.add_value("landlord_name", "Agence Dionis")
        item_loader.add_value("landlord_email", "location@dionis.fr")
        item_loader.add_value("latitude", lat)
        item_loader.add_value("longitude", lng)
        yield item_loader.load_item()