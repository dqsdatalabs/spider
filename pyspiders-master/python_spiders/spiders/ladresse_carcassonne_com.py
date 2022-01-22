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
    name = 'ladresse_carcassonne_com'
    execution_type='testing'
    country='france'
    locale='fr'
    
    custom_settings = {"HTTPCACHE_ENABLED": False}

    def start_requests(self):
        start_urls = [
            {
                "url" : "https://www.ladresse-carcassonne.com/catalog/advanced_search_result.php?action=update_search&C_28=Location&C_28_search=EGAL&C_28_type=UNIQUE&site-agence=&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_27_search=EGAL&C_27_type=TEXT&C_27=1&C_27_tmp=1&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MIN=&C_30_MAX=&30_MIN=&30_MAX=",
                "property_type" : "apartment",
            },
            {
                "url" : "https://www.ladresse-carcassonne.com/catalog/advanced_search_result.php?action=update_search&C_28=Location&C_28_search=EGAL&C_28_type=UNIQUE&site-agence=&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_27_search=EGAL&C_27_type=TEXT&C_27=2%2C17%2C30%2CMaisonVille&C_27_tmp=2&C_27_tmp=17&C_27_tmp=30&C_27_tmp=MaisonVille&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MIN=&C_30_MAX=&30_MIN=&30_MAX=",
                "property_type" : "house"
            },
        ]
        for item in start_urls:
            yield Request(item["url"],
                        callback=self.parse,
                        meta={"property_type": item["property_type"]})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@id='listing_bien']/div[contains(@class,'item')]/div/div/a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta["property_type"]})
        
        next_button = response.xpath("//a[@class='page_suivante']/@href").get()
        if next_button:
            yield Request(response.urljoin(next_button), callback=self.parse, meta={"property_type": response.meta["property_type"]})

# 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url.split("?")[0])
        title_prop = response.xpath("//div[@class='product-description']/h2/text()").get()
        if title_prop and "studio" in title_prop.lower():
            item_loader.add_value("property_type", "studio")
        else:
            item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Ladresse_Carcassonne_PySpider_france")
        
        title = response.xpath("//h1/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
        
        external_id = response.xpath("substring-after(//span[contains(.,'Ref')]/text(),':')").get()
        if external_id:
            item_loader.add_value("external_id", external_id.strip())
        
        address = response.xpath("//span[@class='alur_location_ville']/text()").get()
        if address:
            item_loader.add_value("address", address)
            zipcode = address.split(" ")[0]
            city = address.split(zipcode)[1].strip()
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)
        
        rent = response.xpath("//span[@class='alur_loyer_price']/text()").re_first(r'\d+.\d*')
        if rent:
            rent=rent.replace("\xa0","")
            rent = int(float(rent))
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")
        
        room_count = response.xpath("//li/img[contains(@src,'lit')]/following-sibling::span/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split(" ")[0])
        else:
            room_count = response.xpath("//li/img[contains(@src,'maison')]/following-sibling::span/text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count.split(" ")[0])
        
        square_meters = response.xpath("//li/img[contains(@src,'surface')]/following-sibling::span/text()").get()
        if square_meters:
            square_meters = square_meters.split(" ")[0].replace(",",".")
            item_loader.add_value("square_meters", int(float(square_meters)))
        
        bathroom_count = response.xpath("//li/img[contains(@src,'bain')]/following-sibling::span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        floor = response.xpath("//li/img[contains(@src,'etage')]/following-sibling::span/text()").get()
        if floor:
            item_loader.add_value("floor", floor.split("/")[0])
        
        energy_label = response.xpath("//img[contains(@src,'dpe-')]/@src").get()
        if energy_label:
            energy_label = energy_label.split("dpe-")[1].split(".")[0].upper()
            item_loader.add_value("energy_label", energy_label)
        
        deposit = response.xpath("//span[@class='alur_location_depot']/text()").get()
        if deposit:
            deposit = deposit.split(":")[1].split("€")[0].strip().replace("\xa0","")
            item_loader.add_value("deposit", deposit)
        
        utilities = response.xpath("//span[@class='alur_location_hono_etat_lieux']/text()").get()
        if utilities:
            utilities = utilities.split(":")[1].split("€")[0].strip().replace("\xa0","").replace(",",".")
            item_loader.add_value("utilities", int(float(utilities)))
        
        parking = response.xpath("//li/img[contains(@src,'garage')]/following-sibling::span/text()[.!='0']").get()
        if parking:
            item_loader.add_value("parking", True)
        
        furnished = response.xpath("//li/img[contains(@src,'cuisine')]/following-sibling::span/text()[contains(.,'Meublé')]").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        swimming_pool = response.xpath("//li/img[contains(@src,'piscine')]/following-sibling::span/text()[.!='0']").get()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)
        
        import dateparser
        available_date = response.xpath("//div[@class='content-desc']/text()[contains(.,'Libre au')]").get()
        if available_date:
            available_date = available_date.replace("Libre au","").replace("!","").strip()
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        description = " ".join(response.xpath("//div[@class='content-desc']/text()").getall())
        if description:
            description = re.sub('\s{2,}', ' ', description.strip())
            item_loader.add_value("description", description)
        
        images = [x for x in response.xpath("//ul[@class='slides']//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        latitude_longitude = response.xpath("//script[contains(.,'LatLng(')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('LatLng(')[1].split(',')[0]
            longitude = latitude_longitude.split('LatLng(')[1].split(',')[1].split(')')[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        
        item_loader.add_value("landlord_name", "L'ADRESSE - Carcassonne")
        item_loader.add_value("landlord_phone", "04 68 47 22 22")
        
        yield item_loader.load_item()