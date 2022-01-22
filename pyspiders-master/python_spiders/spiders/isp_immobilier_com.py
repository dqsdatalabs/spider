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
    name = 'isp_immobilier_com'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.isp-immobilier.com/catalog/advanced_search_result.php?action=update_search&search_id=1691768217496248&map_polygone=&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_28_tmp=Location&C_27_search=EGAL&C_27_type=TEXT&C_27=1&C_27_tmp=1&C_34_MIN=&C_34_search=COMPRIS&C_34_type=NUMBER&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MAX=&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&keywords=&C_30_MIN=&C_33_search=COMPRIS&C_33_type=NUMBER&C_33_MIN=&C_33_MAX=&C_34_MAX=&C_36_MIN=&C_36_search=COMPRIS&C_36_type=NUMBER&C_36_MAX=&C_38_MAX=&C_38_MIN=&C_38_search=COMPRIS&C_38_type=NUMBER&C_47_type=NUMBER&C_47_search=COMPRIS&C_47_MIN=&C_94_type=FLAG&C_94_search=EGAL&C_94=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.isp-immobilier.com/catalog/advanced_search_result.php?action=update_search&search_id=1691768217496248&map_polygone=&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_28_tmp=Location&C_27_search=EGAL&C_27_type=TEXT&C_27=2%2C30&C_27_tmp=2&C_27_tmp=30&C_34_MIN=&C_34_search=COMPRIS&C_34_type=NUMBER&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MAX=&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&keywords=&C_30_MIN=&C_33_search=COMPRIS&C_33_type=NUMBER&C_33_MIN=&C_33_MAX=&C_34_MAX=&C_36_MIN=&C_36_search=COMPRIS&C_36_type=NUMBER&C_36_MAX=&C_38_MAX=&C_38_MIN=&C_38_search=COMPRIS&C_38_type=NUMBER&C_47_type=NUMBER&C_47_search=COMPRIS&C_47_MIN=&C_94_type=FLAG&C_94_search=EGAL&C_94=",
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
        for item in response.xpath("//div[@class='visuel-product']/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        next_page = response.xpath("//a[@class='page_suivante']/@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])
        item_loader.add_value("external_source", "Isp_Immobilier_PySpider_france")

        external_id = response.xpath("//span[contains(@itemprop,'name')][contains(.,'Ref.')]//text()").get()
        if external_id:
            external_id = external_id.split(":")[1].strip()
            item_loader.add_value("external_id", external_id)

        title = " ".join(response.xpath("//div[contains(@class,'title-product')]//h1//text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        address = response.xpath("//div[contains(@class,'title-product')]//h1//span//text()").get()
        if address:
            zipcode = address.strip().split(" ")[0]
            city= address.split(zipcode)[1].strip()
            item_loader.add_value("address",address)
            item_loader.add_value("zipcode",zipcode)
            item_loader.add_value("city", city)

        rent = response.xpath("//div[contains(@class,'price-product')]//span[contains(@class,'alur_loyer_price')]//text()").get()
        if rent:
            rent = rent.split("Loyer")[1].split("€")[0].replace("\u00a0","").strip()
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")

        utilities = response.xpath("//span[contains(@class,'alur_location_charges')]//text()").get()
        if utilities:
            utilities = utilities.split(":")[1].split("€")[0].replace(" ","").strip()
            item_loader.add_value("utilities", utilities)

        deposit = response.xpath("//span[contains(@class,'alur_location_depot')]//text()").get()
        if deposit:
            deposit = deposit.split(":")[1].split("€")[0].replace("\u00a0","").strip()
            item_loader.add_value("deposit", deposit)

        room_count = response.xpath("//li[contains(.,'chambre')]//div[contains(@class,'value')]//text()").get()
        if room_count:
            room_count = room_count.strip().split(" ")[0]
            item_loader.add_value("room_count", room_count)
        else:
            room_count = response.xpath("//li[contains(.,'pièce')]//div[contains(@class,'value')]//text()").get()
            if room_count:
                room_count = room_count.strip().split(" ")[0]
                item_loader.add_value("room_count", room_count)
        
        bathroom_count = response.xpath("//li[contains(.,'salle')]//div[contains(@class,'value')]//text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.strip().split(" ")[0]
            item_loader.add_value("bathroom_count", bathroom_count)
        
        square_meters = response.xpath("//li[contains(.,'m²')]//div[contains(@class,'value')]//text()").get()
        if square_meters:
            square_meters = square_meters.strip().split(" ")[0]
            item_loader.add_value("square_meters", square_meters)

        desc = " ".join(response.xpath("//div[contains(@class,'description-product')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        furnished = response.xpath("//span[contains(@class,'alur_location_meuble')]//text()").get()
        if furnished:
            item_loader.add_value("furnished", True)

        images = [x for x in response.xpath("//div[@id='slider_product']//@src").getall()]
        if images:
            item_loader.add_value("images", images)

        latitude_longitude = response.xpath("//script[contains(.,'LatLng')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('LatLng(')[1].split(',')[0]
            longitude = latitude_longitude.split('LatLng(')[1].split(',')[1].split(')')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        item_loader.add_value("landlord_name", "ISP IMMOBILIER")
        
        yield item_loader.load_item()