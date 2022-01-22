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
    name = 'huyzen_be'
    execution_type='testing'
    country='belgium'
    locale='nl'
    def start_requests(self):
        self.start_urls = [
            {"url": "https://huyzen.be/te-huur?type%5B%5D=2&pmin=&pmax=&smin=&bmin=&gmin=&pamin=&omin=&omax=&ogmin=&ogmax=", "property_type": "apartment"},
	        {"url": "https://huyzen.be/te-huur?type%5B%5D=1&pmin=&pmax=&smin=&bmin=&gmin=&pamin=&omin=&omax=&ogmin=&ogmax=", "property_type": "house"},
        ]  # LEVEL 1
        
        yield Request(self.start_urls[0].get('url'),
                        callback=self.parse,
                        meta={'property_type': self.start_urls[0].get('property_type'),
                    })
    
    # 1. FOLLOWING
    def parse(self, response):
        property_type = response.meta.get("property_type")
        page = response.meta.get('page', 2)
        if page == 2:
            urls = response.xpath("//div[@class='gallcell']/a[not(contains(@href,'#'))]/@href").extract()
        else:
            urls = response.xpath("//a[not(contains(@href,'#')) and contains(@class,'estate-small')]/@href").extract()
        
        seen = False
        for item in urls:
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":property_type})
            seen = True

        pagination = f"https://huyzen.be/ajax/estates?page={page}&_=1609758279414"
        if page == 2 or seen:
            yield Request(pagination, self.parse, dont_filter=True, meta={"page":page+1,"property_type":property_type})
        else:
            yield Request(self.start_urls[1].get('url'),
                        callback=self.parse,
                        meta={'property_type': self.start_urls[1].get('property_type'),
                    })
                      
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_xpath("title", "//h1/text()")
        item_loader.add_value("external_source", "Huyzen_PySpider_belgium")

        external_id = "".join(response.xpath("//div[@class='slider-ref']/span[1]/text()").extract())
        if external_id:
            external =  external_id.split(":")[1].strip()
            item_loader.add_value("external_id", external.strip())
        
        room = "".join(response.xpath("//div[@class='data']//div[contains(.,'Slaapkamers')]/following-sibling::div[1]/span/text()").extract())
        if room:
            item_loader.add_value("room_count", room.strip())

        meters = "".join(response.xpath("//div[@class='data']//div[contains(.,'Bewoonbare opp.')]/following-sibling::div[1]/span/text()").extract())
        if meters:
            item_loader.add_value("square_meters", str(int(float(meters.split("m²")[0].strip()))))
        else:
            meters = "".join(response.xpath("//div[@class='data']//div[contains(.,'Totale opp.')]/following-sibling::div[1]/span/text()").extract())
            if meters:
                item_loader.add_value("square_meters", str(int(float(meters.split("m²")[0].strip()))))

        bathroom = "".join(response.xpath("//div[@class='data']//div[contains(.,'Badkamers')]/following-sibling::div[1]/span/text()").extract())
        if bathroom:
            item_loader.add_value("bathroom_count", bathroom.strip())

        address = "".join(response.xpath("//div[@class='slider-info']/span/text()").extract())
        if address:
            if not (("aanvraag" in str(address)) or ("AANVRAAG" in str(address))):
                zipcode = address.split(",")[1].strip().split(" ")[0]
                city = address.split(",")[1].strip().split(" ")[1]
                item_loader.add_value("address", address.strip())
                item_loader.add_value("zipcode", zipcode.strip())
                item_loader.add_value("city", city.strip())

        rent = "".join(response.xpath("//div[@class='data']//div[contains(.,'Prijs')]/following-sibling::div[1]/span/text()").extract())
        if rent:
            price =  rent.split("/")[0]
            item_loader.add_value("rent_string", price.strip())

        utilities = "".join(response.xpath("//div[@class='data']//div[contains(.,'Maandelijkse lasten')]/following-sibling::div[1]/span/text()").extract())
        if utilities:
            uti =  utilities.split(",")[0]
            item_loader.add_value("utilities",uti.strip())

        floor = "".join(response.xpath("//div[@class='data']//div[contains(.,'Verdiepingen')]/following-sibling::div[1]/span/text()").extract())
        if floor:
            item_loader.add_value("floor",floor.strip())

        desc = "".join(response.xpath("//div[@id='beschrijving']/p//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
        else:
            desc = " ".join(response.xpath("//div[@id='beschrijving']/div//text()").extract())
            if desc:
                item_loader.add_value("description", desc.strip())

        images = [x for x in response.xpath("//div[@class='slide imgLiquidFill']/img/@src").extract()]
        if images is not None:
            item_loader.add_value("images", images)      

        floor_plan_images = [x for x in response.xpath("//div[@class='data']/p/a/@href[contains(.,'Indeling')]").extract()]
        if floor_plan_images is not None:
            item_loader.add_value("floor_plan_images", floor_plan_images)     

        elevator = " ".join(response.xpath("//div[@class='data']//div[contains(.,'Lift')]/following-sibling::div[1]/span/text()").getall()).strip()   
        if elevator:
            if "Ja" in elevator:
                item_loader.add_value("elevator", True)


        parking = " ".join(response.xpath("//div[@class='data']//div[contains(.,'Parkeerplaatsen')]/following-sibling::div[1]/span/text()").getall()).strip()   
        if parking:
            if "0" not in parking:
                item_loader.add_value("parking", True)
            else:
                item_loader.add_value("parking", False)

        terrace = " ".join(response.xpath("//div[@class='data']//div[contains(.,'terrassen')]/following-sibling::div[1]/span/text()").getall()).strip()   
        if terrace:
            if "0" not in terrace:
                item_loader.add_value("terrace", True)
            else:
                item_loader.add_value("terrace", False)

        item_loader.add_xpath("landlord_name", "//div[@class='row']/div/h2/text()")
        phone = " ".join(response.xpath("normalize-space(//div[@class='row']/div/p/a/@href[contains(.,'tel')])").getall()).strip()   
        if phone:
            item_loader.add_value("landlord_phone", phone.split(":")[1].strip())

        email = " ".join(response.xpath("normalize-space(//div[@class='row']/div/p/a/@href[contains(.,'mail')])").getall()).strip()   
        if email:
            item_loader.add_value("landlord_email", email.split(":")[1].strip())




        yield item_loader.load_item()