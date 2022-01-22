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
    name = 'century21_gm_tarbes_com'
    execution_type='testing'
    country='france'
    locale='fr'

    custom_settings = {
        "CONCURRENT_REQUESTS" : 2,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": .5,
        "AUTOTHROTTLE_MAX_DELAY": 2,
        "RETRY_TIMES": 3,
        "DOWNLOAD_DELAY": 1,
        "PROXY_ON" : True,
    }

    def start_requests(self):
        start_urls = [
            {"url": "https://www.century21-gm-tarbes.com/annonces/location-appartement/", "property_type": "apartment"},
	        {"url": "https://www.century21-gm-tarbes.com/annonces/location-maison/", "property_type": "house"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'),
                            })

    def parse(self, response):

        for item in response.xpath("//div[@class='js-the-list-of-properties-list-property']"):
            follow_url = response.urljoin(item.xpath(".//a[@title='Voir le détail du bien']/@href").extract_first())
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
        
        next_page = response.xpath("//li[@class='c-the-pagination-bar__item-container'][a[contains(@class,'is-active')]]/following-sibling::li[1]/a/@href").get()
        if next_page:
            yield Request(url=response.urljoin(next_page),callback=self.parse, meta={"property_type" : response.meta.get("property_type")})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get("property_type"))
        item_loader.add_value("external_source", "Century21_Gm_Tarbes_PySpider_france")
  
        external_id = response.xpath("//div[contains(.,'ref')]/text()[normalize-space()]").get()
        if external_id: 
            item_loader.add_value("external_id", external_id.split(':')[-1].strip())
        title = " ".join(response.xpath("//h1/span//text()[normalize-space()]").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        address = response.xpath("//li[@itemprop='itemListElement'][last()]//span[@itemprop='name']/text()").get()
        if address:
            address = " ".join(address.split(" ")[2:]).strip()
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.split("(")[0].strip())
            item_loader.add_value("zipcode", address.split('(')[-1].split(')')[0].strip())

        description = " ".join(response.xpath("//section[@class='c-the-property-detail-description']/div//text()").extract())
        if description:
            item_loader.add_value("description", description.strip())

        item_loader.add_xpath("latitude", "//div[contains(@class,'c-the-map-of-a-property')]/@data-lat")
        item_loader.add_xpath("longitude", "//div[contains(@class,'c-the-map-of-a-property')]/@data-lng")
        
        square_meters = response.xpath("//li[contains(.,'Surface habitable')]/text()[normalize-space()]").get()
        if square_meters:
            square_meters = square_meters.split(":")[1].split("m")[0].split(",")[0].strip()
            item_loader.add_value("square_meters", square_meters)

        room_count = response.xpath("//li[contains(.,'de pièce')]/text()[normalize-space()]").get()
        if room_count:
            room_count = room_count.split(":")[-1].strip()
            item_loader.add_value("room_count", room_count)

        price = "".join(response.xpath("//span[contains(@class,'property-abstract__price')]/text()").getall())
        if price:
            item_loader.add_value("rent_string", price.replace(" ",""))
        
        deposit = response.xpath("//li[contains(.,'de garantie')]/text()[normalize-space()]").get()
        if deposit:
            item_loader.add_value("deposit", deposit.strip(":").split(",")[0].replace(" ",""))

        utilities = response.xpath("//p[contains(.,'provision pour charges')]/text()[normalize-space()]").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split("€")[0].split(":")[-1].split(",")[0].strip())

        balcony = response.xpath("//li[contains(.,'Balcon')]/text()[normalize-space()]").getall()
        if balcony:
            item_loader.add_value("balcony", True)
        elevator = response.xpath("//li[contains(.,'Ascenseur')]/text()[normalize-space()]").getall()
        if elevator:
            item_loader.add_value("elevator", True)
        furnished = response.xpath("//li[contains(.,'meublée')]/text()[normalize-space()]").getall()
        if furnished:
            item_loader.add_value("furnished", True)
        terrace = response.xpath("//li[contains(.,'Terrasse')]/text()[normalize-space()]").getall()
        if terrace:
            item_loader.add_value("terrace", True)
        parking = response.xpath("//li[contains(.,'Parking')]/text()[normalize-space()]").getall()
        if parking:
            item_loader.add_value("parking", True)
        images = [response.urljoin(x) for x in response.xpath("//div[contains(@class,'c-the-detail-images__item')]//img/@src[not(contains(.,'/theme/v6/'))]").getall()]
        if images: 
            item_loader.add_value("images", images)

        energy_label = response.xpath("//div[p[contains(.,'nergie')]]//span[@class='tw-block tw-m-auto']/text()").get()
        if energy_label:
            energy_label = energy_label.split(",")[0]
            item_loader.add_value("energy_label", energy_label_calculate(energy_label))
            
        available_date = response.xpath("//li[contains(.,'Libre le :')]/text()[normalize-space()]").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.split(":")[-1], date_formats=["%d/%m/%Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))

        item_loader.add_xpath("landlord_phone", "//div[contains(@class,'on-mobile')]/@data-click-label")
        item_loader.add_value("landlord_name", "CENTURY 21 GM Immobilier")

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
