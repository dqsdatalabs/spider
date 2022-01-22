# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'atriumsud_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source='Atriumsud_PySpider_france'

    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.atriumsud.gnimmo.com/catalog/advanced_search_result.php?action=update_search&search_id=1715705813073592&map_polygone=&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_27_search=EGAL&C_27_type=TEXT&C_27=1&C_27_tmp=1&C_34_MIN=&C_34_search=COMPRIS&C_34_type=NUMBER&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MAX=&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&keywords=&C_33_MAX=&C_30_MIN=&C_38_MIN=&C_38_search=COMPRIS&C_38_type=NUMBER&C_38_MAX=",
                ],
                "property_type": "apartment"
            },
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
        

        for item in response.xpath("//img[@class='photo-listing']/parent::a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})


    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)

        title = response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title",title)

        external_id = response.xpath("//span[@itemprop='name']//text()[contains(.,'Ref')]").get()
        if external_id:
            external_id = external_id.split("Ref. : ")[1]
            item_loader.add_value("external_id",external_id)

        description = response.xpath("//div[@class='product-description']//text()").get()
        if description:
            item_loader.add_value("description",description)

        rent = response.xpath("//li//div[contains(.,'Loyer mensuel')]//following-sibling::div//text()").get()
        if rent:
            rent = rent.split(" EUR")[0]
            item_loader.add_value("rent",rent)
        item_loader.add_value("currency","EUR")

        utilities = response.xpath("//li//div[contains(.,'Provision sur charges')]//following-sibling::div//text()").get()
        if utilities:
            utilities = utilities.split(" EUR")[0]
            item_loader.add_value("utilities",utilities)

        deposit = response.xpath("//li//div[contains(.,'Dépôt de Garantie')]//following-sibling::div//text()").get()
        if deposit:
            deposit = deposit.split(" EUR")[0]
            item_loader.add_value("deposit",deposit)

        room_count = response.xpath("//li//div[contains(.,'Nombre pièces')]//following-sibling::div//text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)

        bathroom_count = response.xpath("//li//div[contains(.,'Salle(s) de bains')]//following-sibling::div//text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)

        square_meters = response.xpath("//li//div[contains(.,'Surface')]//following-sibling::div//text()").get()
        if square_meters: 
            square_meters = square_meters.split(" m2")[0]
            item_loader.add_value("square_meters",square_meters)

        energy_label = response.xpath("//li//div[contains(.,'Consommation énergie primaire')]//following-sibling::div//text()").get()
        if energy_label: 
            item_loader.add_value("energy_label",energy_label)

        furnished = response.xpath("//li//div[contains(.,'Meublé')]//following-sibling::div//text()[contains(.,'Oui')]").get()
        if furnished: 
            item_loader.add_value("furnished",True)
        else:
            item_loader.add_value("furnished",False)

        latitude_longitude = response.xpath(
            "//script[contains(.,'LatLng')]//text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split(
                'google.maps.LatLng(')[1].split(',')[0]
            longitude = latitude_longitude.split(
                'google.maps.LatLng(')[1].split(',')[1].split(')')[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
            
        images = [x for x in response.xpath("//div[contains(@class,'container-slider-product')]//img//@src").getall()]
        if images:
            item_loader.add_value("images", images)
            
        yield item_loader.load_item()