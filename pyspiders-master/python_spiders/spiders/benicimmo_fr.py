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
    name = 'benicimmo_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.benicimmo.fr/fr/liste.htm?ope=2&filtre=2",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.benicimmo.fr/fr/liste.htm?ope=2&filtre=8",
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
        page = response.meta.get("page", 2)
        for item in response.xpath("//img[@class='anti-cheat']/../@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta['property_type']})
        
        next_page = response.xpath("//span[@class='PageSui']/span/@id").get()
        if next_page:
            p_url = response.url.split("&page=")[0] + f"&page={page}"
            yield Request(
                p_url,
                callback=self.parse,
                meta={'property_type': response.meta['property_type'], "page":page+1}
            )

    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source","Benicimmo_PySpider_"+ self.country)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        
        title = response.xpath("//title/text()").get()
        item_loader.add_value("title", re.sub('\s{2,}', ' ',title.strip()))
        
        address = response.xpath("//h2[@class='detail-bien-ville']/text()").get()
        if address:
            item_loader.add_value("address", address)
            zipcode = address.split(" ")[-1]
            city = address.split(zipcode)[0].strip()
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode.replace("(","").replace(")",""))
        
        square_meters = response.xpath("//ul/li/span[contains(@class,'surface')]/parent::li/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.strip().split(" ")[0])
        
        room_count = response.xpath("//ul/li/span[contains(@class,'piece')]/parent::li/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip().split(" ")[0])
        
        rent = response.xpath("//div[contains(@class,'prix')]/text()").get()
        if rent:
            price = rent.strip().split("€")[0].strip().replace(" ","")
            item_loader.add_value("rent", price)
        item_loader.add_value("currency", "EUR")
        
        external_id = "+".join(response.xpath("//span[@itemprop='productID']//text()").getall())
        if external_id:
            item_loader.add_value("external_id", external_id.split("+")[1])
        
        desc = "".join(response.xpath("//span[@itemprop='description']//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        energy_label = response.xpath("//img/@src[contains(.,'nrj')]").get()
        if energy_label:
            try:
                energy_label = energy_label.split("w-")[1].split("-")[0]
                item_loader.add_value("energy_label", energy_label)
            except: pass
        
        images = [x for x in response.xpath("//div[@class='scrollpane']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        latitude = response.xpath("//li[contains(@class,'lat')]/text()").get()
        longitude = response.xpath("//li[contains(@class,'lng')]/text()").get()
        if latitude or longitude:
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
        
        if "Disponible le" in desc:
            available_date = desc.split("Disponible le")[1].split(".")[0].strip()
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        if "salle de bain" in desc.lower():
            bathroom_count = desc.lower().split("salle de bain")[0].strip().split(" ")[-1]
            if "une" in bathroom_count:
                item_loader.add_value("bathroom_count", "1")
        
        utilities = response.xpath("//span[contains(@itemprop,'description')]//text()[contains(.,'de provision')]").get()
        if utilities:
            utilities = utilities.split("euros de provisions")[0].split("+")[1].strip()
            item_loader.add_value("utilities", utilities)
            
        deposit = response.xpath("//span[contains(@itemprop,'description')]//text()[contains(.,'Dépôt de garantie')]").get()
        if deposit:
            deposit = deposit.split("Dépôt de garantie")[1].split("euros")[0].strip().replace(":","")
            item_loader.add_value("deposit", deposit)
        
        item_loader.add_value("landlord_name", "BENICIMMO VOTRE AGENCE IMMOBILIERE")
        item_loader.add_value("landlord_phone", "04 94 51 65 49")
        item_loader.add_value("landlord_email", "contact@benicimmo.fr")
        
        yield item_loader.load_item()