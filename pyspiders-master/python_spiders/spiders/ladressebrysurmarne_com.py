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
    name = 'ladressebrysurmarne_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source = "Ladressebrysurmarne_com_PySpider_france_fr"
    
    def start_requests(self):
        start_urls = [
            {"url": "https://www.ladresse-brysurmarne.com/catalog/result_carto.php?action=update_search&C_28=Location&C_28_search=EGAL&C_28_type=UNIQUE&site-agence=&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_27_search=EGAL&C_27_type=TEXT&C_27=1&C_27_tmp=1&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MIN=&C_30_MAX=&30_MIN=&30_MAX=", "property_type": "apartment"},
            {"url": "https://www.ladresse-brysurmarne.com/catalog/result_carto.php?action=update_search&C_28=Location&C_28_search=EGAL&C_28_type=UNIQUE&site-agence=&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_27_search=EGAL&C_27_type=TEXT&C_27=2&C_27_tmp=2&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MIN=&C_30_MAX=&30_MIN=&30_MAX=", "property_type": "house"},
            {"url": "https://www.ladresse-brysurmarne.com/catalog/result_carto.php?action=update_search&C_28=Location&C_28_search=EGAL&C_28_type=UNIQUE&site-agence=&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_27_search=EGAL&C_27_type=TEXT&C_27=17&C_27_tmp=17&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MIN=&C_30_MAX=&30_MIN=&30_MAX=", "property_type": "house"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//div[@class='products-cell']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
        
            
# 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", self.external_source)
        title = " ".join(response.xpath("//h1//text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])

        meters = "".join(response.xpath("//ul/li[2]/span[@class='critere-value']/text()[contains(.,'m²')]").extract())
        if meters:
            item_loader.add_value("square_meters", int(float(meters.split("m²")[0].replace(",",".").strip())))

        room = "".join(response.xpath("//ul/li/span[@class='critere-value']/text()[contains(.,'Pièces')]").extract())
        if room:
            item_loader.add_value("room_count", room.split("Pièces")[0])
        
        bathroom_count=response.xpath("//ul/li/img[contains(@src,'bain')]/following-sibling::span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        price = "".join(response.xpath("//div[@class='product-price'][1]/div/span/text()").extract())
        if price :
            item_loader.add_value("rent_string", price.replace(" ","").replace("\xa0","").strip())

        address = "".join(response.xpath("//div[@class='row']/h1/text()").extract())
        if address:
            addr = address.split("à")[1]
            item_loader.add_value("address", addr.strip())
            item_loader.add_value("city", addr.strip())

        external_id = "".join(response.xpath("//span[@itemprop='name']/text()[contains(.,'Ref.')]").extract())
        if external_id:
            item_loader.add_value("external_id", external_id.split(":")[1].strip())
        
        floor=response.xpath("//li[img[contains(@src,'etage')]]/span/text()").get()
        if floor:
            item_loader.add_value("floor", floor.split("/")[0])
        
        desc = "".join(response.xpath("//div[@class='content-desc']/text()").extract())
        item_loader.add_value("description", desc.strip())

        label = "".join(response.xpath("//div[@class='product-dpe'][1]/div/@class").extract())
        if label :
            item_loader.add_value("energy_label", label.split(" ")[1].split("-")[1].upper())

        images = [response.urljoin(x)for x in response.xpath("//div[@class='sliders-product']//img/@src").extract()]
        if images:
            item_loader.add_value("images", images)

        deposit=response.xpath("//span[contains(@class,'depot')]/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split(":")[1].split("€")[0].replace(" ","").replace("\xa0",""))
        
        utilities=response.xpath("//span[contains(@class,'charges')]/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split(":")[1].split("€")[0].replace(" ",""))
        
        furnished=response.xpath("///ul/li/span[@class='critere-value']/text()[contains(.,'Meublée') or contains(.,'Aménagée') or contains(.,'équipée')]").extract_first()
        if furnished:
            item_loader.add_value("furnished", True)

        parking=response.xpath("//ul/li/span[@class='critere-value']//preceding::img/@src[contains(.,'garage')]").extract_first()
        if parking:
            item_loader.add_value("parking", True)
        
        latitude_longitude="".join(response.xpath("//script[contains(.,'lat')]/text()").extract())
        if latitude_longitude:
            lat=latitude_longitude.split('LatLng(')[1].split(',')[0].strip()
            lng=latitude_longitude.split('LatLng(')[1].split(',')[1].split(");")[0]
            item_loader.add_value("latitude",lat)
            item_loader.add_value("longitude", lng)

        item_loader.add_value("landlord_phone", "06 24 29 69 74")
        item_loader.add_value("landlord_name", "Ladresse Saintpierre")
        item_loader.add_value("landlord_email", "civm@ladresse.com")
        yield item_loader.load_item()