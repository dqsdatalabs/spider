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
    name = 'costadelsolproperties_es'
    execution_type='testing'
    country='spain'
    locale='en'
    thousand_separator = ','
    scale_separator = '.'

    def start_requests(self):
        start_urls = [
            {"url": "http://www.costadelsol-properties.es/luxury-rentals_l5_t2_ien.html"}
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//table[contains(@title,'Click here')]//a/@href").extract():
            follow_url = response.urljoin(item)
            if "sale" not in follow_url:
                if "apartment" in follow_url:
                    property_type = "apartment"
                    yield Request(follow_url, callback=self.populate_item, meta={'property_type': property_type})
                elif "house" in follow_url or "villa" in follow_url:
                    property_type = "house"
                    yield Request(follow_url, callback=self.populate_item, meta={'property_type': property_type})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Costadelsolproperties_PySpider_"+ self.country + "_" + self.locale)
        item_loader.add_xpath("title", "//small/text()")
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        ext_id=response.xpath("//tr/td[contains(.,'C.I.R.T.')]/text()[normalize-space()]").get()
        if ext_id:
            item_loader.add_value("external_id", ext_id.replace(":","").replace("Tourist Licence;","").strip())
        desc = "".join(response.xpath("//td/p/text()").extract())
        desc = desc.replace('\n', '').replace('\r', '').replace('\t', '').replace('\xa0', '')
        if desc:
            item_loader.add_value("description", desc.strip())
        if "balcony" in desc.lower():
            item_loader.add_value("balcony", True)
        if "washing machine" in desc.lower():
            item_loader.add_value("washing_machine", True)
        desc=desc.lower()
        if "deposit" in desc.lower():
            deposit=desc.split("deposit")[1].replace("final","-").replace("eurfinal","-").replace("extra","-").replace("cleaning","-")
            deposit=deposit.split("-")[0].replace(":"," ").replace("eur","").strip().split(" ")[-1].replace("..........","").replace(",","")
            if "." in deposit:
                deposit=deposit.split(".")[0].replace(",","")
            else:
                deposit=deposit.replace(",","")
            if deposit.isdigit():
                item_loader.add_value("deposit", deposit)
        if "pets" in desc.lower():
            pets=desc.split("pets")[1].replace("=","").replace(":","").strip().split(" ")[0]
            if "not" in pets:
                item_loader.add_value("pets_allowed", False)
            elif "considered" in pets or "included" in pets or "max" in pets:
                item_loader.add_value("pets_allowed", True)
        
        meters = "".join(response.xpath("//tr[td[. ='Built Area:']]/td[2]/text()").extract())
        meters2=response.xpath("//tr[td[. ='Plot Area:']]/td[2]/text()").get()
        meters3=response.xpath("//tr[td[. ='Useful Area:']]/td[2]/text()").get()
        if meters:
            meters = str(int(float(meters.split("m")[0].strip().replace(',','.'))))
            item_loader.add_value("square_meters", meters)
        elif meters2:
            square_meters = str(int(float(meters2.split("m")[0].strip().split(",")[0].replace(".",""))))
            item_loader.add_value("square_meters", square_meters)
        elif meters3:
            square_meters = str(int(float(meters3.split("m")[0].strip().split(",")[0].replace(".",""))))
            item_loader.add_value("square_meters", square_meters)
        elif "m2" in desc:
            item_loader.add_value("square_meters",desc.split("m2")[0].strip().split(" ")[-1])
        else:
            meters4 = "".join(response.xpath("substring-before(substring-after(//td[@class='v4 pbig']//b/text(),'just'),' mtrs ')").extract())
            if meters4:
                item_loader.add_value("square_meters",meters4.strip())
        

        rent = "".join(response.xpath("//tr[td[.='Week']]/td/text()").extract())
        rent2=response.xpath("//td/small/text()[contains(.,'€')]").get()
        if "Week" in rent :
            price=rent.split("€")[1].strip().replace(",","")
            item_loader.add_value("rent", str(int(price)*4))
        elif rent:
            price=rent.split("€")[1].strip().replace(",","")
            item_loader.add_value("rent", price)
        elif rent2:
            price=rent2.split("€")[0].strip().split(" ")[-1].replace(".","")
            item_loader.add_value("rent", price)
        
        item_loader.add_value("currency","EUR")
            
        address = "".join(response.xpath("//td[@class='adr']//text()").extract())
        if address :
            item_loader.add_value("address", address.strip().replace("\n","").replace("\r","").replace("\t",""))

        city=response.xpath("//td/span[@class='region']//text()").get()
        if city:
            item_loader.add_value("city",city.strip().replace("(","").replace(")",""))
            
        zipcode=response.xpath("//td/span[@class='postal-code']//text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode)
        
        images = [response.urljoin(x)for x in response.xpath("//div[@id='links']/a/img/@src").extract()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))

        room = "".join(response.xpath("//tr[@class='v4']/td/text()[contains(.,'Bedroom')]").extract())
        if room:
            item_loader.add_value("room_count", room.split("Bedroom")[0].replace("-","").strip())

        bathroom = "".join(response.xpath("//tr[@class='v4']/td/text()[contains(.,'Bathroom')]").extract())
        if bathroom:
            item_loader.add_value("bathroom_count", bathroom.split("Bathroom")[0].replace("-","").strip())
        
        latitude=response.xpath("//span[@class='latitude']/text()").get()
        longitude=response.xpath("//span[@class='longitude']/text()").get()
        if latitude or longitude:
            item_loader.add_value("latitude",latitude)
            item_loader.add_value("longitude",longitude)
        
        floor = "".join(response.xpath("//td[contains(.,'Floors')]/b/parent::td/text()[2]").extract())
        if floor:
            floor = floor.strip().split('F')[0].strip()
            item_loader.add_value("floor", floor.strip().split("Floors")[0])
        else:
            floor = "".join(response.xpath("//table[@class='tdetalle']//tr[1][contains(.,'Floor')]/td/text()").extract())
            if floor:
                floor = floor.strip().split('Floor')[0].strip().replace("th","").replace("st","").replace("In Urban Area                  , Urbanization","" )
                item_loader.add_value("floor",floor )


        swimming_pool = "".join(response.xpath("//tr[@class='v4']/td/text()[contains(.,'Swimming')]").extract())
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)

        parking = "".join(response.xpath("//a//tr[td[contains(.,'Garage')]]//text()").extract())
        if parking:
            item_loader.add_value("parking", True)

        terrace = "".join(response.xpath("//td[@class='v4']//text()[contains(.,'Terrace')]").extract())
        if terrace:
            item_loader.add_value("terrace", True)
        else:
            terrace2 = "".join(response.xpath("//p[@align ='justify'][contains(.,'Terrace')]/text()").extract())
            if terrace2:
                item_loader.add_value("terrace", True)


        elevator = "".join(response.xpath("//td[@class='v4']//text()[contains(.,'Lift')]").extract())
        if elevator:
            item_loader.add_value("elevator", True)


        Furnished = "".join(response.xpath("//td[@class='v4']/b/text()[contains(.,'Furnished')]").extract())
        if Furnished:
            item_loader.add_value("furnished", True)
        

        dishwasher = "".join(response.xpath("//p[@align ='justify'][contains(.,'Dishwasher')]/text()").extract())
        if dishwasher:
            item_loader.add_value("dishwasher", True)

        washing_machine = "".join(response.xpath("//p[@align ='justify'][contains(.,'Washing machine')]/text()").extract())
        if washing_machine:
            item_loader.add_value("washing_machine", True)



        item_loader.add_value("landlord_phone", "+31 646 290 161")
        item_loader.add_value("landlord_name", "Costadelsolproperties")
        item_loader.add_value("landlord_email", "info@cds-properties.com")
        status = response.xpath("//b[contains(.,'Holiday')]/text()").get()
        if not status:
            yield item_loader.load_item()