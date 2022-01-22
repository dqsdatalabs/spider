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

class MySpider(Spider):
    name = 'sedei_fr' 
    execution_type='testing'
    country='france'
    locale='fr'
    thousand_separator = ','
    scale_separator = '.' # LEVEL 1

    def start_requests(self):
        start_urls = [ 
            {"url": "https://www.sedei.fr/location/?&type=All"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={"type":url.get('type')})

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='page_search']//a"):   
            href = item.xpath("@href").get()
            property_type = item.xpath("span[1]/text()").get()
            if property_type:
                property_type = property_type.strip()
                if 'chambre' in property_type.lower():
                    property_type = 'room'
                elif 'appartement' in property_type.lower() or 'studio' in property_type.lower() or 'type-ii' in property_type.lower():
                    property_type = 'apartment'
                elif 'maison' in property_type.lower():
                    property_type = 'house'
                else: 
                    property_type = None 
            else:
                property_type = None
            if property_type:
                follow_url = response.urljoin(href)
                yield Request(follow_url, callback=self.populate_item, meta={"property_type": property_type})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        property_type = response.meta.get("property_type")
        item_loader.add_value("property_type", property_type)

        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", "Sedei_PySpider_"+ self.country + "_" + self.locale)

        title="".join(response.xpath("//h1//span//text()").getall())
        if title:
            item_loader.add_value("title", title.strip().replace("\r","").replace("\t",""))
    
        item_loader.add_xpath("external_id", "substring-after(//div[@class='title_product']//li[contains(.,'Mandat')]/text(),'Mandat ')")    
        
        # address = response.xpath("//strong[contains(.,'Adresse')]/following-sibling::span/text()").getall()
        # add_str = ''
        # if address:
        #     for a in address:
        #         add_str += a.strip() + ' '
        #     item_loader.add_value("address", add_str)
        # if add_str:
        #     city = add_str.split(' -')[-1].strip()
        #     item_loader.add_value("city", city)
 
        # if add_str:
        #     zipcode = add_str.split(' -')[0].strip().split(' ')[-1].strip()
        #     item_loader.add_value("zipcode", zipcode)

        address=response.xpath("//span[@class='loc_bien']//text()").get()
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("city", address)
        else:
            address =" ".join(response.xpath("//li[@class='address']/text()").getall())
            if address:
                city=address.split(" ")[-1]
                item_loader.add_value("address", address)
                item_loader.add_value("city", city)

        square_meters = response.xpath("//img[contains(@src,'m2')]/following-sibling::span/text()").get()
        if square_meters:
            square_meters = str(int(float(square_meters.split('m')[0].strip())))
            item_loader.add_value("square_meters", square_meters)

        description =" ".join(response.xpath("//h2[contains(.,'Description')]/following-sibling::p/text()").getall()) 
        if description:
            item_loader.add_value("description", description.strip())
            if "meublé" in description.lower():
                item_loader.add_value("furnished", True)

            
        room_count = response.xpath("//img[contains(@src,'pi')]/following-sibling::span/text()").get()
        room=response.xpath("//h1//span[@class='type_bien']/text()").get()
        if room_count:
            room_count = room_count.strip().split(' ')[0]
            item_loader.add_value("room_count", room_count)
        elif room and "studio" in room:
            item_loader.add_value("room_count","1")
        elif "chambre" in description:
            item_loader.add_value("room_count", description.split("chambre")[0].strip().split(" ")[-1])
        
        rent = response.xpath("//span[@itemprop='price']/text()").get()
        currency = response.xpath("//span[@itemprop='priceCurrency']/text()").get()
        if rent and currency:
            rent = str(int(float(rent.strip()))) + ' ' + currency.strip()
            item_loader.add_value("rent_string", rent)      


        images = [x for x in response.xpath("//div[@id='product_gallery']//img/@src[not(contains(.,'/interface/annonce_default-1'))]").getall()]
        if images:
            item_loader.add_value("images", images)
            # item_loader.add_value("external_images_count", str(len(images)))
        
        deposit = response.xpath("//li[contains(.,'Dépot')]/text()").get()
        if deposit:
            deposit = str(int(float(deposit.split(':')[1].split('€')[0].strip())))
            if deposit != "0":
                item_loader.add_value("deposit", deposit)

        utilities = response.xpath("//li[contains(.,'Charges')]/text()[.!='Charges : 0.00 €']").get()
        if utilities:
            utilities = str(int(float(utilities.split(':')[1].split('€')[0].strip())))
            if utilities != "0":
                item_loader.add_value("utilities", utilities)
            else:
                utilities = response.xpath("//br/following-sibling::text()[contains(.,'Loyer Hors Charges')]").get()
                if utilities:
                    utilities = str(int(float(utilities.split(':')[-1].split('€')[0].strip())))
                    if utilities != "0":
                        item_loader.add_value("utilities", utilities)
        else:
            utilities = response.xpath("//br/following-sibling::text()[contains(.,'Loyer Hors Charges')]").get()
            if utilities:
                utilities = str(int(float(utilities.split(':')[-1].split('€')[0].strip())))
                if utilities != "0":
                    item_loader.add_value("utilities", utilities)

        energy_label = response.xpath("//img[contains(@src,'dpe')]/following-sibling::span/strong/text()").get()
        if energy_label:
            energy_label = energy_label.strip()
            item_loader.add_value("energy_label", energy_label)        

        floor = response.xpath("//img[contains(@src,'etage')]/following-sibling::span/text()").get()
        if floor:
            floor = floor.strip().split(' ')[0]
            item_loader.add_value("floor", floor.replace("e",""))

        landlord_name = response.xpath("substring-after(//div[@id='agence_contact']/h2/text(),':')").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name.strip())

        landlord_phone = response.xpath("//li[@class='phone']/text()").get()
        if landlord_phone:
            landlord_phone = landlord_phone.split('.')[1].strip()
            item_loader.add_value("landlord_phone", landlord_phone)

        yield item_loader.load_item()

