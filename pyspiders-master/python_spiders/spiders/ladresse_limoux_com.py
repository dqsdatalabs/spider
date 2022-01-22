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
    name = 'ladresse_limoux_com'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {"url": "https://www.ladresse-limoux.com/catalog/result_carto.php?action=update_search&C_28=Location&C_28_search=EGAL&C_28_type=UNIQUE&site-agence=&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_27_search=EGAL&C_27_type=TEXT&C_27=1&C_27_tmp=1&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MIN=&C_30_MAX=&30_MIN=&30_MAX=", "property_type": "apartment"},
            {"url": "https://www.ladresse-limoux.com/catalog/result_carto.php?action=update_search&C_28=Location&C_28_search=EGAL&C_28_type=UNIQUE&site-agence=&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_27_search=EGAL&C_27_type=TEXT&C_27=2&C_27_tmp=2&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MIN=&C_30_MAX=&30_MIN=&30_MAX=", "property_type": "house"},
            {"url": "https://www.ladresse-limoux.com/catalog/result_carto.php?action=update_search&C_28=Location&C_28_search=EGAL&C_28_type=UNIQUE&site-agence=&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_27_search=EGAL&C_27_type=TEXT&C_27=17&C_27_tmp=17&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MIN=&C_30_MAX=&30_MIN=&30_MAX=", "property_type": "house"},
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

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])
        item_loader.add_value("external_source", "Ladresse_Limoux_PySpider_france")

        external_id = response.xpath("//span[contains(@itemprop,'name')][contains(.,'Ref.')]//text()").get()
        if external_id:
            external_id = external_id.split(":")[1].strip()
            item_loader.add_value("external_id", external_id)

        title = "".join(response.xpath("//h1//text()").getall())
        if title:
            item_loader.add_value("title", title)

        address = response.xpath("//span[contains(@class,'alur_location_ville')]//text()").get()
        if address:
            item_loader.add_value("address", address)
            zipcode = address.split(" ")[0]
            address = address.split(zipcode)[1].strip()
            item_loader.add_value("city", address)
            item_loader.add_value("zipcode", zipcode)

        desc = " ".join(response.xpath("//div[contains(@class,'content-desc')]//text()[not (contains(.,'Nos honoraires')) and not(contains(.,'Imprimer'))]").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        rent = response.xpath("//div[contains(@class,'prix')]//span[contains(@class,'alur_loyer_price')]//text()").get()
        if rent:
            rent = rent.split("Loyer")[1].split("€")[0].replace("\xa0","").strip()
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")

        utilities = response.xpath("//span[contains(@class,'alur_location_charges')]//text()").get()
        if utilities:
            utilities = utilities.split(":")[1].split("€")[0].strip().replace(" ","")
            item_loader.add_value("utilities", utilities)

        deposit = response.xpath("//span[contains(@class,'alur_location_depot')]//text()").get()
        if deposit:
            deposit = deposit.split(":")[1].split("€")[0].strip().replace(" ","").replace("\xa0","")
            item_loader.add_value("deposit", deposit)

        furnished = response.xpath("//span[contains(@class,'alur_location_meuble')]//text()").get()
        if furnished:
            item_loader.add_value("furnished", True)

        square_meters = response.xpath("//img[contains(@src,'surface')]/following-sibling::span//text()").get()
        if square_meters:
            square_meters = square_meters.split("m²")[0].strip()
            item_loader.add_value("square_meters", int(float(square_meters)))

        room_count = response.xpath("//img[contains(@src,'lit')]/following-sibling::span//text()").get()
        if room_count:
            room_count = room_count.split(" ")[0].strip()
            item_loader.add_value("room_count", room_count)
        else:
            room_count = response.xpath("//img[contains(@src,'maison')]/following-sibling::span//text()").get()
            if room_count:
                room_count = room_count.split(" ")[0].strip()
                item_loader.add_value("room_count", room_count)

        bathroom_count = response.xpath("//img[contains(@src,'bain')]/following-sibling::span//text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)

        parking = response.xpath("//img[contains(@src,'garage')]/following-sibling::span//text()").get()
        if parking:
            item_loader.add_value("parking", True)

        swimming_pool = response.xpath("//img[contains(@src,'piscine')]/following-sibling::span//text()").get()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)

        floor = response.xpath("//img[contains(@src,'etage')]/following-sibling::span//text()").get()
        if floor:
            floor = floor.split("/")[0]
            item_loader.add_value("floor", floor)

        energy_label = response.xpath("//img[contains(@class,'img-dpe')]//@src[not(contains(.,'empty'))]").get()
        if energy_label:
            energy_label = energy_label.split("-")[1].split(".")[0].upper()
            if energy_label in ["A","B","C","D","E","F","G"]:
                item_loader.add_value("energy_label", energy_label)

        images = [x for x in response.xpath("//div[contains(@id,'large_slider_product')]//ul[contains(@class,'slides')]//li//img//@src").getall()]
        if images:
            item_loader.add_value("images", images)

        latitude_longitude = response.xpath("//script[contains(.,'maps.LatLngBounds')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('LatLng(')[1].split(',')[0]
            longitude = latitude_longitude.split('LatLng(')[1].split(',')[1].split(')')[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        item_loader.add_value("landlord_name", "L'ADRESSE Limoux")
        item_loader.add_value("landlord_phone", "04 68 31 57 05")

        yield item_loader.load_item()