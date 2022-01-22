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
    name = 'newdealproperties_be'
    execution_type='testing'
    country='france'
    locale='fr'
    thousand_separator = ','
    scale_separator = '.' 

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://newdeal.immo/en/estate/?PurposeIds=2&CategoryIds=2&ZipCodes=&Min=0&Max=5000",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://newdeal.immo/en/estate/?PurposeIds=2&CategoryIds=1&ZipCodes=&MaxRooms=None&Min=0&Max=5000",
                ],
                "property_type" : "house",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='col-12 col-md-6 col-xl-4 mb-6']/a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

        next_page = response.urljoin(response.xpath("//li[@class='pagination__item']/a[@class='pagination__link--arrow--next']/@href").extract_first())
        if next_page:
            # print("------",next_page)
            yield Request(next_page, callback=self.parse, meta={"property_type":response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Newdealproperties_Be_PySpider_france")
        item_loader.add_value("external_id",response.url.split("/")[-2])        
        title = response.xpath("//h1/text()").get()
        item_loader.add_value("title", title)
        
        address = " ".join(response.xpath("//p[@class='estates__paragraph mb-7']//text()").getall())
        if address:
            address = re.sub("\s{2,}", " ",address)
            item_loader.add_value("address", address)
            item_loader.add_xpath("city", "//div[h4[.='Municipality:']]/span/text()")
            zipcode = address.split(",")[-2].strip().split(" ")[0]
            item_loader.add_value("zipcode", zipcode)
        
        rent = response.xpath("//div[h4[.='Price:']]/span/text()").get()
        if rent:
            item_loader.add_value("rent_string", rent)      
        
        bathroom_count = "".join(response.xpath("//div[h4[.='Bathroom(s)']]/p/text()[.!='0']").getall())
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())

        room_count = "".join(response.xpath("//div[h4[.='Room(s)']]/p/text()").getall())
        if room_count:
            item_loader.add_value("room_count", room_count.strip())

        energy_label = "".join(response.xpath("//div[h4[.='E spec (kwh/m²/year):']]/span/text()").getall())
        if energy_label:
            item_loader.add_value("energy_label", energy_label.strip())
        
        square_meters = "".join(response.xpath("//div[h4[.='Living area:']]/span/text()").getall())
        if square_meters:
            item_loader.add_value("square_meters", square_meters.replace("m","").strip()) 

        # floor = response.xpath("//div/strong[contains(.,'Etage')]/following-sibling::text()").get()
        # if floor:
        #     item_loader.add_value("floor", floor)

        lat_lng = response.xpath("substring-before(substring-after(//script//text()[contains(.,'lat')],'lat'),',')").get()
        if lat_lng:
            item_loader.add_value("latitude", lat_lng.replace('"',"").replace(":","").strip())
            longitude = response.xpath("substring-before(substring-after(//script//text()[contains(.,'lat')],'lng'),',')").get()
            item_loader.add_value("longitude", longitude.replace('"',"").replace(":","").strip())
        
        terrace = response.xpath("//div[h4[.='Terrace']]/p/text()").get()
        if terrace:
            if "yes" in terrace.lower():
                item_loader.add_value("terrace", True)
            else:
                item_loader.add_value("terrace", False)

        furnished = response.xpath("//div[@class='listing_detail col-md-4']/strong[contains(.,'Furnished')]/following-sibling::text()").get()
        if furnished:
            if "oui" in furnished.lower():
                item_loader.add_value("furnished", True)
            elif "non" in furnished.lower():
                item_loader.add_value("furnished", False)
        parking = response.xpath("//div[h4[.='Parking']]/p/text()").get()
        if parking:
            if "yes" in parking.lower():
                item_loader.add_value("parking", True)
            else:
                item_loader.add_value("parking", False)
                
        description = " ".join(response.xpath("//div[@class='col-12 col-md-8']/p/text()").getall())
        if description:
            description = re.sub('\s{2,}', ' ', description.strip())
            item_loader.add_value("description", description)
        
        # if "chambre" in description:
        #     if description.split("chambre")[0].strip().split(" ")[-1].isdigit():
        #         item_loader.add_value("room_count", description.split("chambre")[0].strip().split(" ")[-1])
        #     elif description.split("chambre")[1].strip().split(" ")[-1].isdigit():
        #         item_loader.add_value("room_count", description.split("chambre")[1].strip().split(" ")[-1])
    
        available_date = ""
        if "Disponible au" in description:
            available_date = description.split("Disponible au")[1].split(".")[0].strip()
        elif "libre au" in description.lower():
            available_date = description.lower().split("libre au")[1].split(".")[0].strip()
        
        import dateparser
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        images = [x for x in response.xpath("//div[@id='tiny-slider-estates']//img//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        landlord_phone = response.xpath("//div[@class='d-flex flex-column mb-4 mb-xl-0']//div//a//text()").get()
        if landlord_phone:
                item_loader.add_value("landlord_phone", landlord_phone.strip())
        else:
            item_loader.add_value("landlord_phone", "+32 (0)2 725 20 05")

        landlord_name = response.xpath("//div[@class='d-flex flex-column mb-4 mb-xl-0']//span//text()").get()
        if landlord_name:
                item_loader.add_value("landlord_name", landlord_name.strip())
        else:
            item_loader.add_value("landlord_name", "New Deal Properties")
        
        item_loader.add_value("landlord_email", "info@newdealproperties.com")
 
        yield item_loader.load_item()