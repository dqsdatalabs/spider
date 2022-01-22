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
    name = 'lourdes_madameimmo_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source='Lourdesmadameimmo_PySpider_france_fr'
    thousand_separator = ','
    scale_separator = '.'
    def start_requests(self):
        start_urls = [
            {"url": "https://www.madameimmo.com/immobilier-annonces/?location_text=&geo_lat=&geo_long=&geo_radius=6&types=-1&cat=location&min_bedrooms=0&max_bedrooms=14&min_amountrooms=0&max_amountrooms=20&min_price=0&max_price=1500000&min_area=0&max_area=1000", "property_type": "apartment"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                             callback=self.parse,
                             meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        for follow_url in response.xpath("//div[@class='property-box-image']/a/@href").extract():
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", self.external_source)

        title = " ".join(response.xpath("//h2[@class='entry-title']/text()").extract())
        item_loader.add_value("title", title)
        if "COMMERCIAL" in title:
            pass
        else:

            item_loader.add_value("external_link", response.url)
            external_id = "".join(response.xpath("//tr[th[.='Reference annonce : ']]/td//text()").extract())
            item_loader.add_value("external_id", external_id.strip())

            description = "".join(response.xpath("//div[@class='entry-content']/p/text()").extract())
            if description:
                item_loader.add_value("description", description.strip())
            
            if "piscine" in description.lower():
                item_loader.add_value("swimming_pool", True)
            
            address = ""
            city = response.xpath("//tr[th[.='Ville : ']]/td/span/strong/text()").get()
            if city:
                address = address + city + " ("
                item_loader.add_value("city", city)

            zipcode = response.xpath("//tr[th[.='Code Postal : ']]/td/span/strong/text()").get()
            if zipcode:
                address = address + zipcode + ")"
                item_loader.add_value("zipcode", zipcode)
            
            if address != "":
                item_loader.add_value("address", address)
                
            if "honoraires" in description.lower():
                utilities=description.lower().split("honoraires")[1].split(":")[1].strip().split(" ")[0]
                item_loader.add_value("utilities", utilities)
            
            if "garantie" in description.lower():
                garantie=description.lower().split("garantie")[1].replace(":","").strip().replace(" ","")
                item_loader.add_value("deposit", garantie)
            
            energy_label = response.xpath("//span[@class='diagnostic-number']/text()").get()
            if energy_label:
                try:
                    energy_label = energy_label_calculate(energy_label)
                    item_loader.add_value("energy_label", energy_label)
                except:
                    pass
            
            item_loader.add_value("property_type", response.meta.get('property_type'))
            
            square_meters = "".join(response.xpath("//div[contains(@class,'entry-summary-tabs')]//li[@class='property-label-areasize']//div/span[@class='label-content']/text()").extract())
            if square_meters:
                item_loader.add_value("square_meters", int(float(square_meters.strip())))
            
            room_count = "".join(response.xpath("//div[contains(@class,'entry-summary-tabs')]//li[@class='property-label-bedrooms']//div/span[@class='label-content']/text()").extract())
            if room_count:
                item_loader.add_value("room_count", room_count.strip())
            else:
                room_count = response.xpath("//li[@class='property-label-amountrooms']//div[@class='info-meta']/span/text()").re_first(r'\d+')
                if room_count:
                    item_loader.add_value("room_count", room_count.strip())

            bathroom_count=response.xpath(
                "//div[@class='info-meta']//span[contains(.,'Salles')]/parent::div/span[@class='label-content']/text()"
                ).get()
            if bathroom_count:
                item_loader.add_value("bathroom_count", bathroom_count.strip().split(" ")[0])
            
            floor = response.xpath("//tr[th[.='Etage : ']]/td/span/strong/text()").get()
            if floor:
                item_loader.add_value("floor", floor.strip())
            elif "\u00e9tage" in description.lower():
                floor=description.split("\u00e9tage")[0].strip().split(" ")[-1].replace("ème","")
                if floor.isdigit():
                    item_loader.add_value("floor", floor)
            
            images = [x.split("('")[1].split("')")[0] for x in response.xpath('//div[@class="swiper-wrapper"]/div/div/@style').getall()]
            if images:
                item_loader.add_value("images", images)
            
            price = "".join(response.xpath("//div[@class='property-single-info']/div/div[@class='property-price']/span/text()").extract())
            if price:
                price = price.split("€")[0].replace(".","")
                item_loader.add_value("rent",price )
                item_loader.add_value("currency", "EUR")

            terrace = "".join(response.xpath("//tr[th[.='Ascenseur : ']]/td/span/strong/text()").extract()).strip()
            if terrace:
                item_loader.add_value("elevator", True)

            terrace = "".join(response.xpath("//tr[th[.='Balcon : ']]/td/span/strong/text()").extract()).strip()
            if terrace:
                if "oui" in terrace:
                    item_loader.add_value("balcony", True)
                else:
                    item_loader.add_value("balcony", False)

            terrace = "".join(response.xpath("//tr[th[.='Terrasse : ']]/td/span/strong/text()").extract()).strip()
            if terrace:
                item_loader.add_value("terrace", True)

            parking = "".join(response.xpath("//tr[th[.='Parking : ']]/td/span/strong/text()").extract()).strip()
            if parking:
                item_loader.add_value("parking", True)
            
            latitude_longitude = response.xpath("//a/@href[contains(.,'maps')]").get()
            if latitude_longitude:
                lat = latitude_longitude.split("&ll=")[1].split(",")[0]
                lng = latitude_longitude.split("&ll=")[1].split(",")[1].split("&")[0]
                item_loader.add_value("latitude", lat)
                item_loader.add_value("longitude", lng)
            
            item_loader.add_xpath("landlord_phone", "//div[@class='agent-box-phone']/span//text()")
            item_loader.add_xpath("landlord_email", "//div[@class='agent-box-email']/a/span/text()")
            item_loader.add_xpath("landlord_name", "//h4[@class='agent-box-title']/a/text()")

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