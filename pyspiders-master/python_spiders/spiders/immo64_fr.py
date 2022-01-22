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
    name = 'immo64_fr'
    execution_type = 'testing'
    country = 'france'
    locale ='fr'
    external_source = 'GroupeImmo_PySpider_france'
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.immo64.fr/catalog/advanced_search_result.php?action=update_search&search_id=1704889819389014&map_polygone=&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_27_search=EGAL&C_27_type=TEXT&C_27=1&C_27_tmp=1&C_34_MIN=&C_34_search=COMPRIS&C_34_type=NUMBER&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MAX=&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&keywords=&C_33_MAX=&C_30_MIN=&C_38_MIN=&C_38_search=COMPRIS&C_38_type=NUMBER&C_38_MAX=",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://www.immo64.fr/catalog/advanced_search_result.php?action=update_search&search_id=1704889819389014&map_polygone=&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_27_search=EGAL&C_27_type=TEXT&C_27=2&C_27_tmp=2&C_34_MIN=&C_34_search=COMPRIS&C_34_type=NUMBER&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MAX=&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&keywords=&C_33_MAX=&C_30_MIN=&C_38_MIN=&C_38_search=COMPRIS&C_38_type=NUMBER&C_38_MAX="
                ],
                "property_type": "house"
            }
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,
                    meta={'property_type': url.get('property_type')}
                )

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//div[@class='products-link']/a/@href").extract():
            follow_url = response.urljoin(item)            
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)
        external_id = re.search(r'search_id=(\d+)&', response.url)
        if external_id:
            item_loader.add_value("external_id", external_id.group(1))
        title=response.xpath("//title//text()").get() 
        if title:
            item_loader.add_value("title", title)
        
        rent = response.xpath("//span[@class='alur_loyer_price']/text()").get()
        if rent:
            rent = re.sub(r"\D", "", rent)
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")
        
        address = response.xpath("//span[@class='alur_location_ville']/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
        
        # city = response.xpath("//span[@class='alur_location_ville']/text()").get()
        # if city:
        #     item_loader.add_value("city", address.split(" ")[-1])
        item_loader.add_value("city", "PAU")   
        
        zipcode = response.xpath("//span[@class='alur_location_ville']/text()").re_first(r"\d+")
        if zipcode:
            item_loader.add_value("zipcode", zipcode)


        desc = "".join(response.xpath("//div[@class='description-product']/text()").getall())
        if desc:
            desc = re.sub(r"\s{2,}", "", desc)
            item_loader.add_value("description", desc)

        deposit = "".join(response.xpath("//span[@class='alur_location_depot']/text()").re(r"\d+"))
        if deposit:
            item_loader.add_value("deposit", deposit)
        
        utilities = response.xpath("//span[@class='alur_location_charges']/text()").re_first(r"\d+")
        if utilities:
            item_loader.add_value("utilities", utilities)
        
        room_count = response.xpath("//div[@class='value']/text()[contains(.,'pièce')]").re_first(r"\d+")
        if room_count:
            item_loader.add_value("room_count", room_count)
        else:
            room_count = response.xpath("//div[contains(.,'chambre')]/text()").re_first(r"\d+")
            if room_count:
                item_loader.add_value("room_count", room_count)
        
        bathroom_count = response.xpath("//div[@class='value']/text()[contains(.,'de bain')]").re_first(r"\d+")
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)

        square_meter = response.xpath("//ul[@class='list-caracts']//div[contains(.,'m²')]/text()").re_first(r"\d+")
        if square_meter:
            item_loader.add_value("square_meters", square_meter)
        
        furnished = response.xpath("//span[@class='alur_location_meuble']/text()").get()
        if furnished and 'meublé' in furnished.lower():
            item_loader.add_value("furnished", True)

        images = [response.urljoin(i) for i in response.xpath("//div[contains(@class,'item-slider')]/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
        
        lat_lng = response.xpath("//script[contains(.,'LatLng')]").re_first(r"LatLng\((\d+.\d+, -*\d+.\d+)\)")
        if lat_lng:
            item_loader.add_value("latitude", lat_lng.split(",")[0].strip())
            item_loader.add_value("longitude", lat_lng.split(",")[1].strip())
        
        item_loader.add_value("landlord_name", "IMMO64 CIPNORD")
        item_loader.add_value("landlord_phone", "0559046928")
        
        yield item_loader.load_item()