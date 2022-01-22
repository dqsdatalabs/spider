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
import dateparser

class MySpider(Spider):
    name = 'ibt_associes_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source = "Ibt_Associes_PySpider_france"
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.ibt-associes.com/catalog/advanced_search_result.php?action=update_search&search_id=&map_polygone=&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_28_tmp=Location&C_27_search=EGAL&C_27_type=TEXT&C_27=1&C_27_tmp=1&C_34_MIN=&C_34_search=COMPRIS&C_34_type=NUMBER&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MAX=&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&keywords=&C_30_MIN=&C_33_search=COMPRIS&C_33_type=NUMBER&C_33_MIN=&C_33_MAX=&C_34_MAX=&C_36_MIN=&C_36_search=COMPRIS&C_36_type=NUMBER&C_36_MAX=&C_38_MAX=&C_38_MIN=&C_38_search=COMPRIS&C_38_type=NUMBER&C_47_type=NUMBER&C_47_search=COMPRIS&C_47_MIN=&C_94_type=FLAG&C_94_search=EGAL&C_94=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.ibt-associes.com/catalog/advanced_search_result.php?action=update_search&search_id=&map_polygone=&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_28_tmp=Location&C_27_search=EGAL&C_27_type=TEXT&C_27=2&C_27_tmp=2&C_34_MIN=&C_34_search=COMPRIS&C_34_type=NUMBER&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MAX=&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&keywords=&C_30_MIN=&C_33_search=COMPRIS&C_33_type=NUMBER&C_33_MIN=&C_33_MAX=&C_34_MAX=&C_36_MIN=&C_36_search=COMPRIS&C_36_type=NUMBER&C_36_MAX=&C_38_MAX=&C_38_MIN=&C_38_search=COMPRIS&C_38_type=NUMBER&C_47_type=NUMBER&C_47_search=COMPRIS&C_47_MIN=&C_94_type=FLAG&C_94_search=EGAL&C_94=",
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
        for item in response.xpath("//div[@class='products-link']/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta['property_type']})
        
        next_page = response.xpath("//a[@class='page_suivante']/@href").get()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse,
                meta={'property_type': response.meta['property_type']}
            )

    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source","Ibt_Associes_PySpider_"+ self.country)
        item_loader.add_value("external_link", response.url.split("?")[0])

        title = "".join(response.xpath("//div[@class='title-product']/h1/text()").getall())
        if title:
            item_loader.add_value("title", title.strip())
        
        address = response.xpath("//div[@class='title-product']/h1/span/text()").get()
        if address:
            item_loader.add_value("address", address)
            zipcode = address.split(" ")[0]
            city = address.split(zipcode)[1].strip()
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)
            
        rent= response.xpath("//div[@class='price-product']//span[1]/text()").get()
        if rent:
            price = rent.split("€")[0].split("Loyer")[1].strip().replace(" ","").replace("\xa0","")
            item_loader.add_value("rent", math.ceil(float(price)))
        item_loader.add_value("currency", "EUR")
        
        room_count = response.xpath("//ul/li[contains(.,'chambre')]/div[@class='value']/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split(" ")[0])
        else:
            room_count = response.xpath("//ul/li[contains(.,'pièce')]/div[@class='value']/text()").get()
            item_loader.add_value("room_count", room_count.split(" ")[0])            
        
        square_meters = response.xpath("//ul/li[contains(.,'m²')]/div[@class='value']/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split(" ")[0])
        
        bathroom_count = response.xpath("//ul/li[contains(.,'salle')]/div[@class='value']/text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.split(" ")[0]
            if bathroom_count.isdigit():
                item_loader.add_value("bathroom_count", bathroom_count)
        
        desc = "".join(response.xpath("//div[@class='description-product']/text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        if "studio" in desc.lower():
            item_loader.add_value("property_type", "studio")
        else:
            item_loader.add_value("property_type", response.meta.get('property_type'))
        
        date2 = ""
        if "Disponible le" in desc:
            available_date = desc.split("Disponible le")[1].split(".")[0].strip()
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            date2 = date_parsed.strftime("%Y-%m-%d")
        elif "Disponible \u00e0 partir du" in desc:
            available_date = desc.split("Disponible \u00e0 partir du")[1].split(".")[0].strip()
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            date2 = date_parsed.strftime("%Y-%m-%d")

        if date2:
            item_loader.add_value("available_date", date2)
            
        if "\u00e9tage" in desc:
            floor = desc.split("\u00e9tage")[0].strip().split(" ")[-1].replace("ème","").replace("er","")
            if floor.isdigit():
                item_loader.add_value("floor", floor)
                
        images = [ x for x in response.xpath("//div[@class='item-slider']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        lat_lng = response.xpath("//script[contains(.,'LatLng')]/text()").get()
        if lat_lng:
            latitude = lat_lng.split("LatLng(")[1].split(",")[0]
            longitude = lat_lng.split("LatLng(")[1].split(",")[1].split(")")[0].strip()
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
        
        external_id = response.xpath("//span[contains(.,'Ref')]/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(":")[1].strip())
                
        deposit = response.xpath("//span[@class='alur_location_depot']/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split(":")[1].split("€")[0].replace("\xa0","").replace(" ","").strip())
        
        utilities = response.xpath("//span[@class='alur_location_charges']/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split(":")[1].split("€")[0].strip())
        
        item_loader.add_value("landlord_name", "IBT GESTION")
        item_loader.add_value("landlord_phone", "04.74.78.47.00")
        

        yield item_loader.load_item()