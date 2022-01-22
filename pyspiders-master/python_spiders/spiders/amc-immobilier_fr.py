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
import dateparser
class MySpider(Spider):
    name = 'amc-immobilier_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source="AmcImmobilier_PySpider_france"
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.amc-immobilier.fr/fr/location/1/",
                ],
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse)
    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//a[@class='button']/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source",self.external_source)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        title = response.xpath("//div[@class='row crumbs']/following-sibling::h1/text()").get()
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ',title.strip()))
        
        property_type=response.xpath("//div[@class='row crumbs']/following-sibling::h1/text()").get()
        if property_type:
            if "Maison" in property_type:
                item_loader.add_value("property_type","house")
            if "Appartement" in property_type:
                item_loader.add_value("property_type","apartment")
                
        adres=response.xpath("//div[@class='row crumbs']/following-sibling::h1/text()").get()
        if adres:
            item_loader.add_value("address",adres.split("Pièce(s)")[-1].strip())
        zipcode=response.xpath("//div[@class='row crumbs']/following-sibling::h1/text()").get()
        if zipcode:
            item_loader.add_value("zipcode",zipcode.split("Pièce(s)")[-1].strip().split("(")[1].split(")")[0])
        rent=response.xpath("//p[@class='h4']/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("€")[0].strip().replace(" ",""))
        item_loader.add_value("currency","EUR")
        external_id=response.xpath("//span[contains(.,'Ref')]/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id.split(":")[-1].strip())
        square_meters=response.xpath("//img[contains(@src,'size')]/following-sibling::div/p/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split(".")[0].strip())
        room_count=response.xpath("//img[contains(@src,'bedroom')]/following-sibling::div/p/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.strip())
        bathroom_count=response.xpath("//img[contains(@src,'shower')]/following-sibling::div/p/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count.strip())
        description="".join(response.xpath("//div[@class='block-item']//text()").getall())
        if description:
            item_loader.add_value("description",description.replace("\t","").replace("\n","").replace("Description","").strip())
        utilities=response.xpath("//p[contains(.,'Honoraires locatai')]/span/text()").get()
        if utilities:
            item_loader.add_value("utilities",utilities.split("€")[0].strip())
        deposit=response.xpath("//p[contains(.,'Dépôt de garantie')]/span/text()").get()
        if deposit:
            item_loader.add_value("deposit",deposit.split("€")[0].strip())
        terrace=response.xpath("//p[contains(.,'Terrasse')]/span/text()").get()
        if terrace and terrace=="OUI":
            item_loader.add_value("terrace",True)
        parking=response.xpath("//p[contains(.,'Parking')]/span/text()").get()
        if parking:
            item_loader.add_value("parking",True)
        images=[response.urljoin(x.split("url('")[1].split(");")[0]) for x in response.xpath("//a[@class='thumb']/div/@style").getall()]
        if images:
            item_loader.add_value("images",images)
        latitude=response.xpath("//div[@class='large-12 columns no-marg annonce ah']/@data-lat").extract()
        if latitude:
            item_loader.add_value("latitude",latitude)
        longitude=response.xpath("//div[@class='large-12 columns no-marg annonce ah']/@data-lng").extract()
        if longitude:
            item_loader.add_value("longitude",longitude)
        item_loader.add_value("landlord_name","AMC IMMOBILIER")

        yield item_loader.load_item()