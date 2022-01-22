# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from html.parser import HTMLParser
import dateparser

class MySpider(Spider):
    name = 'tlimmobilier_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source='Tlimmobilier_PySpider_france_fr'
    def start_requests(self):
        start_urls = [
            {"url": "http://www.tlimmobilier.com/immobilier/pays/locations/france.htm"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                             callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//div[@id='recherche-resultats-listing']/div"):
            follow_url = response.urljoin(item.xpath(".//a[@itemprop='url']/@href").extract_first())
            property_type = item.xpath(".//p/span[contains(@class,'h2-like')]/text()").get()
            if "appartement" in property_type.lower():
                property_type = "apartment"
                yield Request(follow_url, callback=self.populate_item, meta={'property_type': property_type})
            elif "maison" in property_type.lower():
                property_type = "house"
                yield Request(follow_url, callback=self.populate_item, meta={'property_type': property_type})
        

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_source", "Tlimmobilier_PySpider_"+ self.country + "_" + self.locale)

        prop_type = response.meta.get('property_type')
        
        title = response.xpath("normalize-space(//h1/text()[1])").extract_first() + " " + response.xpath("normalize-space(//h1/text()[2])").extract_first()
        item_loader.add_value("title", title)

        item_loader.add_value("external_link", response.url)

        external_id = response.xpath("//div[@class='row-fluid zone-ligne']/div[1]/div[2]/span/text()").get()
        if external_id:
            external_id = external_id.split(':')[1].strip()
        item_loader.add_value("external_id", external_id)

        result = response.xpath("//div[@class='row-fluid zone-ligne']/div[1]/h1/text()[2]").get()
        if result:
            address = result.strip()
            zipcode = result.strip().split('(')[1].strip(')')
        item_loader.add_value("address", address)
        item_loader.add_value("city", address.split("(")[0].strip())
        item_loader.add_value("zipcode", zipcode)

        item_loader.add_value("property_type", prop_type)

        square_meters = response.xpath("//div[contains(text(),'m²')]/text()").get()
        if square_meters:
            square_meters = square_meters.strip().replace('\xa0m²', "").split(',')[0].strip()
        item_loader.add_value("square_meters", square_meters)

        room_count = response.xpath("//div[contains(text(),'pièce')]/text()").get()
        if room_count:    
            item_loader.add_value("room_count", room_count.strip().split(" ")[0].strip())
      
        bathroom_count = response.xpath("//div[contains(.,'Salle')]/parent::li/div[2]/text()").get()
        if bathroom_count:    
            item_loader.add_value("bathroom_count", bathroom_count.strip())
        
        deposit = response.xpath("//div[@class='row-fluid']/strong[contains(.,'garantie')]/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split(":")[1].split("€")[0].strip())
        
        utilities = response.xpath("//div[@class='hidden-phone']//text()[(contains(.,'charges') or contains(.,'Charges')) and not(contains(.,'Loyer'))]").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split(":")[1].split("€")[0].strip())
        
        energy_label = response.xpath(
            "//div/img[contains(@src,'dpe_hab')]/parent::div/p[contains(@class,'diagLettre diag')]"
            ).get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label)
        
        lat_lng=response.xpath("//script[contains(.,'AGLATITUDE')]/text()").get()
        if lat_lng:
            lat_lng=lat_lng.split("AGLATITUDE")[1]
            lat=lat_lng.split('LATITUDE: "')[1].split('"')[0]
            lng=lat_lng.split('LONGITUDE: "')[1].split('"')[0]
            if lat and lng:
                item_loader.add_value("latitude", lat)
                item_loader.add_value("longitude", lng)
        
        price = "".join(response.xpath("//div[1]/div[contains(@class,'bloc-detail-prix')]//text()").extract())
        if price:
            item_loader.add_value("rent_string", price.replace("\xa0","."))

        description = ""
        description = response.xpath("//p[@itemprop='description']/text()").getall()
        desc_html = ''      
        if description:
            for d in description:
                desc_html += d
            filt = HTMLFilter()
            filt.feed(desc_html)
            item_loader.add_value("description", filt.text)
        
        if "DISPONIBLE" in desc_html:
            available_date=desc_html.split("DISPONIBLE")[1]
            if available_date:
                try:
                    date_parsed = dateparser.parse(
                        available_date, date_formats=["%m/%Y"]
                    )
                    if date_parsed:
                        date2 = date_parsed.strftime("%Y-%m")
                    else: 
                        date2 = None
                except AttributeError:
                    available_date=available_date.split("~")[0].strip()
                    date_parsed = dateparser.parse(
                        available_date, date_formats=["%d%m/%Y"]
                    )
                    if date_parsed:
                        date2 = date_parsed.strftime("%Y-%m-%d")
                    else: 
                        date2 = None
                if date2:
                    item_loader.add_value("available_date", date2)
        
        images = [x for x in response.xpath("//div[@class='nivoSlider z100']/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)

        floor = response.xpath("//div[.='Etage']/parent::li/div[2]/text()").get()
        if floor:
            floor = floor.strip()
        item_loader.add_value("floor", floor)

        elevator = response.xpath("//div[.='Ascenseur']/parent::li/div[2]/text()").get()
        if elevator:
            if elevator.strip().lower().startswith("oui"):
                elevator = True
            else:
                elevator = False
        item_loader.add_value("elevator", elevator)

        balcony = response.xpath("//div[.='Balcon']/parent::li/div[2]/text()").get()
        if balcony:
            if int(balcony.strip()) > 0:
                balcony = True
            else:
                balcony = False
        item_loader.add_value("balcony", balcony)

        parking = response.xpath("//div[.='Parkings']/parent::li/div[2]/text()").get()
        if parking:
            if int(parking.strip()) > 0:
                item_loader.add_value("parking", True)
            else:
                item_loader.add_value("parking", False)
        else:
            description = "".join(description)
            if "garage" in description.lower().strip() or "parking" in description.lower().strip():
                item_loader.add_value("parking", True)

        

        landlord_phone = response.xpath("//p[@id='detail-agence-tel']/text()[2]").get()
        if landlord_phone:
            landlord_phone = landlord_phone.strip()
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("landlord_name","TLIMMOBILIER")

        landlord_email = response.xpath("normalize-space(//span[@id='emailAgence']/text())").get()
        if landlord_email:
            item_loader.add_value("landlord_email", "info@treviorta.be")


        yield item_loader.load_item()

class HTMLFilter(HTMLParser):
    text = ''
    def handle_data(self, data):
        self.text += data