# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy.linkextractors import LinkExtractor
from scrapy import Request
from scrapy.selector import Selector
from python_spiders.items import ListingItem
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re
import dateparser

class MySpider(Spider):
    name = "lecobel_vaneau_be"
    execution_type = 'testing'
    country = 'belgium'
    locale = 'en'
    external_source = "Lecobelvaneau_PySpider_belgium_en"
    
    def start_requests(self):
        start_urls = [
            {"url": "https://www.lecobel-vaneau.be/en/vaneau-search/search?field_ad_type[eq][]=renting&field_property_type[eq][]=38&limit=28&mode=list&offset=0&offset_additional=0&search_page_id=580", "property_type": "apartment"},
            {"url": "https://www.lecobel-vaneau.be/en/vaneau-search/search?field_ad_type[eq][]=renting&field_property_type[eq][]=37&limit=28&mode=list&offset=0&offset_additional=0&search_page_id=580", "property_type": "house"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                                callback=self.parse,
                                meta={'property_type': url.get('property_type'),
                                        'base_url':url.get('url')})

    # 1. FOLLOWING
    def parse(self, response):

        offset = response.meta.get("offset", 28)
        base_url = response.meta.get("base_url")

        data = json.loads(response.body)
        content = data["html"]
        sel = Selector(text=content, type="html")

        seen = False
        for item in sel.xpath(
            "//div[contains(@class,'results-items')]//a[contains(@class,'link__property full-link')]/@href"
        ).extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, 
                            callback=self.populate_item,
                            meta={"property_type":response.meta.get("property_type")})
            seen = True

        if seen:
            page = f"&offset={offset}&"
            url = base_url.replace("&offset=0&", page) 
            yield Request(url, 
                            callback=self.parse, 
                            meta={"offset": offset + 28, 
                            'base_url':base_url,"property_type":response.meta.get("property_type")})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", self.external_source)
        #item_loader.add_css("title", "h1")
        title = response.xpath("//h1//text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        desc = "".join(response.xpath("//div[@class='description']/text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
        
        if "Available on" in desc:
            available_date = desc.split("Available on")[1].split("!")[0].strip()
            if available_date:
                date_parsed = dateparser.parse(
                    available_date, date_formats=["%d/%m/%Y"]
                )
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
        
        item_loader.add_value("external_link", response.url)
        rent = response.xpath(
            "//div[@class='informations__main']/span[@class='price']/text()[contains(., '€')]"
        ).get()
        if rent:
            item_loader.add_value("rent", rent.replace(" ", "").split("€")[0])
        item_loader.add_value("currency","EUR")
        
        item_loader.add_value("property_type", response.meta.get("property_type"))

        item_loader.add_xpath("latitude", "//section[@id='maps']/div/@data-lat")
        item_loader.add_xpath("longitude", "//section[@id='maps']/div/@data-lng")
        external_id = response.xpath("//div[@class='specifications']/div[contains(.,'Reference : ')]/span//text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.replace("\n","").strip())
        
        square = response.xpath(
            "//div[@class='specifications']/div[contains(.,'Surface : ')]/span/text()"
        ).get()
        if square:
            if "sq ft" in square:
                square_title = square.split("sq ft")[0]
                sqm = str(int(float(square_title) * 0.09290304))
                item_loader.add_value("square_meters", sqm)
            
        
        floor = "".join(
            response.xpath(
                "normalize-space(//div[@class='specifications']/div[contains(.,'Floor :')]/span/text())"
            ).extract()
        )
        if floor:
            item_loader.add_value("floor", floor.strip())

        terrace = response.xpath(
            "normalize-space(//div[@class='specifications']/div[contains(.,'Terrace : ')]/span/text())"
        ).get()
        if terrace:
            if terrace == "Yes" or terrace == "Oui":
                item_loader.add_value("terrace", True)
            else:
                item_loader.add_value("terrace", False)

        images = [
            response.urljoin(x)
            for x in response.xpath(
                "//div[contains(@class,'background-image')]/picture/source[1]/@srcset"
            ).extract()
        ]
        item_loader.add_value("images", images)

        terrace = response.xpath(
            "normalize-space(//div[@class='specifications']/div[contains(.,'Parking')]/span/text())"
        ).get()
        if terrace:
            item_loader.add_value("parking", True)

        terrace = response.xpath(
            "normalize-space(//div[@class='specifications']/div[contains(.,'Elevator')]/span/text())"
        ).get()
        if terrace:
            if terrace == "Yes" or terrace == "Oui":
                item_loader.add_value("elevator", True)
            else:
                item_loader.add_value("elevator", False)
        if "washing machine" in desc.lower():
            item_loader.add_value("washing_machine", True)
        if "swimming pool" in desc.lower():
            item_loader.add_value("swimming_pool", True)
        if "furnished" in desc.lower() and "unfurnished" not in desc.lower():
            item_loader.add_value("furnished", True)
        if "balcony" in desc.lower():
            item_loader.add_value("balcony", True)
            

        
        attr=response.xpath("//script[contains(.,'postalCode')]/text()").get()
        if attr:
            city=attr.split('addressLocality":')[1].split(',')[0].replace('"','').strip()
            zipcode=attr.split('postalCode":')[1].split(',')[0].replace('"','').strip()
            country=attr.split('addressCountry":')[1].split(',')[0].replace('"','').strip()
            address=attr.split('streetAddress":')[1].split('}')[0].replace('"','').strip()
            if city:
                item_loader.add_value("city", city)
            if zipcode:
                item_loader.add_value("zipcode", zipcode)
            if address or country:
                item_loader.add_value("address", address+" "+city+" "+country+" "+zipcode)
        
        room = response.xpath("normalize-space(//div[@class='specifications']/div[contains(.,'Bedroom')]/span/text())").extract_first()
        if room and room != "0": 
            item_loader.add_value(
                "room_count",
                room,
            )
        else:
            room_studio = response.xpath("//h1//text()[contains(.,'studio')]").extract_first()
            if room_studio:
                item_loader.add_value("room_count","1")

        
        bathroom=response.xpath("normalize-space(//div[@class='specifications']/div[contains(.,'Bathroom')]/span/text())").get()
        if bathroom:
            item_loader.add_value("bathroom_count", bathroom)
        
        charges=response.xpath("//div[@class='specifications']/div[contains(.,'Loads') or contains(.,'Load')]/span/text()[contains(.,'€')]").get()
        if charges:
            item_loader.add_value("utilities", charges.split("€")[0].strip().replace(" ", ""))
        
        energy_label=response.xpath("//div[@class='letters']//div[contains(@class,'sticker')]/text()").get()
        if energy_label:
            e_label=energy_label_calculate(energy_label)
            item_loader.add_value("energy_label",e_label )

        phone = response.xpath('//div[@class="phone"]/text()').get()
        if phone:
            item_loader.add_value("landlord_phone", phone.replace("ph", ""))

        email = response.xpath('//div[@class="agency-mail icon-mail"]/a/@href').get()
        if email:
            item_loader.add_value("landlord_email", email.replace("mailto:", ""))

        name = response.xpath("//div[@class='name']//text()").get()
        if name:
            item_loader.add_value("landlord_name", name.strip())
        else: item_loader.add_value("landlord_name", "Lecobel Vaneau")
        
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