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
from datetime import datetime
import dateparser

class MySpider(Spider):
    name = 'regiemialon_com'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.regiemialon.com/nos-annonces/?type_recherche=LO&natures=appartement&saisonnier=0&neuf=0&numero_page=1",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.regiemialon.com/nos-annonces/?type_recherche=LO&natures=maison-individuelle&saisonnier=0&neuf=0&numero_page=1",
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
        
        seen = False
        for item in response.xpath("//div[contains(@class,'annonce-box')]/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta['property_type']})
            seen = True
        
        if page == 2 or seen:
            p_url = response.url.split("&numero_page=")[0] + f"&numero_page={page}"
            yield Request(
                p_url,
                callback=self.parse,
                meta={'property_type': response.meta['property_type'], "page":page+1}
            )

    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Regiemialon_PySpider_france")
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        title = response.xpath("//h1/text()").get()
        if title:
            item_loader.add_value("title", title)
        
        address = response.xpath("//tr/td[contains(.,'Adresse')]/following-sibling::td/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            city = address.split(",")[-1].strip()
            item_loader.add_value("city", city)
        
        rent = response.xpath("//tr/td[contains(.,'toutes')]/following-sibling::td/span[@itemprop='price']/text()").get()
        if rent:
            item_loader.add_value("rent", rent.replace(" ",""))
            item_loader.add_value("currency", "EUR")
        
        room_count = response.xpath("//p[contains(.,'Pièce')]/span/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        else:
            room_count = response.xpath("//p[contains(.,'Chambr')]/span/text()").get()
            item_loader.add_value("room_count", room_count)
        
        square_meters = response.xpath("//p[contains(.,'Surface')]/span/text()").get()
        if square_meters:
            square_meters = square_meters.split(" ")[0]
            item_loader.add_value("square_meters", int(float(square_meters)) )
        
        bathroom_count = response.xpath("//p[contains(.,'Salle de bain')]/span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        else:
            item_loader.add_xpath("bathroom_count", "//p[@class='txtSmallBlocsDetailLoc']/text()[contains(.,'Salle')]/following-sibling::span/text()")
        
        floor = response.xpath("//p[contains(.,'Etage')]/span/text()").get()
        if floor:
            item_loader.add_value("floor", floor)
        
        utilities = response.xpath("//tr/td[contains(.,'Charges')]/following-sibling::td/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split("€")[0].strip())
        
        deposit = response.xpath("//tr/td[contains(.,'garantie')]/following-sibling::td/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split("€")[0].strip())
        
        desc = "".join(response.xpath("//p[@itemprop='description']//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        images = [x for x in response.xpath("//div[contains(@class,'item-image')]//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        available_date = response.xpath("//tr/td[contains(.,'Dispon')]/following-sibling::td/text()").get()
        if available_date:
            if "Immediate" in available_date:
                available_date = datetime.now().strftime("%Y-%m-%d")
                item_loader.add_value("available_date", available_date)
            else:
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
                       
        external_id = response.xpath("//h4[contains(.,'Réf')]/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(":")[1].strip())
        
        energy_label = response.xpath("//div/img[contains(@src,'energie')]/parent::div/div[@class='class_curseur']/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label_calculate(energy_label))
        
        latitude = response.xpath("//script[contains(.,'lat')]/text()").re_first(r'"lat":"(\d+.\d+)"')
        longitude = response.xpath("//script[contains(.,'lat')]/text()").re_first(r'"lng":"(\d+.\d+)"')
        if latitude and longitude:
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
        
        elevator = response.xpath("//p[contains(.,'Ascenseur')]/span/text()[.!='0']").get()
        if elevator:
            item_loader.add_value("elevator", True)
        
        item_loader.add_value("landlord_name", "REGIE MIALON")
        item_loader.add_value("landlord_phone", "04 73 42 27 65")
        
        yield item_loader.load_item()

def energy_label_calculate(energy_number):
    energy_number = int(energy_number)
    energy_label = ""
    if energy_number <= 50:
        energy_label = "A"
    elif energy_number > 50 and energy_number <= 90:
        energy_label = "B"
    elif energy_number > 90 and energy_number <= 150:
        energy_label = "C"
    elif energy_number > 150 and energy_number <= 230:
        energy_label = "D"
    elif energy_number > 230 and energy_number <= 330:
        energy_label = "E"
    elif energy_number > 330 and energy_number <= 450:
        energy_label = "F"
    elif energy_number > 450:
        energy_label = "G"
    return energy_label