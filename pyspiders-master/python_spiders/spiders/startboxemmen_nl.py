# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import dateparser

class MySpider(Spider):
    name = 'startboxemmen_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    external_source = "Start_Box_PySpider_netherlands"

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.startboxemmen.nl/woningaanbod/huur?street_postcode_city=&status=&rent_price_from=&rent_price_till=&accordions=on&construction_year_from=&construction_year_till=&living_area_from=&living_area_till=&kind_of_house%5BEENGEZINSWONING%5D=on&kind_of_house%5BHERENHUIS%5D=on&kind_of_house%5BVILLA%5D=on&kind_of_house%5BLANDHUIS%5D=on&kind_of_house%5BBUNGALOW%5D=on&kind_of_house%5BWOONBOERDERIJ%5D=on&kind_of_house%5BGRACHTENPAND%5D=on&kind_of_house%5BWOONBOOT%5D=on&kind_of_house%5BSTACARAVAN%5D=on&kind_of_house%5BWOONWAGEN%5D=on&object_type%5BWOONHUIS%5D=on&object_type%5BBOUWGROND%5D=on&house_type%5BVRIJSTAANDE_WONING%5D=on&house_type%5BGESCHAKELDE_WONING%5D=on&house_type%5BTWEE_ONDER_EEN_KAPWONING%5D=on&house_type%5BTUSSENWONING%5D=on&house_type%5BHOEKWONING%5D=on&house_type%5BEINDWONING%5D=on&house_type%5BHALFVRIJSTAANDE_WONING%5D=on&house_type%5BGESCHAKELDE_TWEE_ONDER_EEN_KAPWONING%5D=on&house_type%5BVERSPRINGEND%5D=on",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.startboxemmen.nl/woningaanbod/huur?street_postcode_city=&status=&rent_price_from=&rent_price_till=&accordions=on&construction_year_from=&construction_year_till=&living_area_from=&living_area_till=&kind_of_house%5BEENGEZINSWONING%5D=on&kind_of_house%5BHERENHUIS%5D=on&kind_of_house%5BVILLA%5D=on&kind_of_house%5BLANDHUIS%5D=on&kind_of_house%5BBUNGALOW%5D=on&kind_of_house%5BWOONBOERDERIJ%5D=on&kind_of_house%5BGRACHTENPAND%5D=on&kind_of_house%5BWOONBOOT%5D=on&kind_of_house%5BSTACARAVAN%5D=on&house_type%5BVRIJSTAANDE_WONING%5D=on&house_type%5BGESCHAKELDE_WONING%5D=on&house_type%5BTWEE_ONDER_EEN_KAPWONING%5D=on&house_type%5BTUSSENWONING%5D=on&house_type%5BHOEKWONING%5D=on&house_type%5BEINDWONING%5D=on&house_type%5BHALFVRIJSTAANDE_WONING%5D=on&house_type%5BGESCHAKELDE_TWEE_ONDER_EEN_KAPWONING%5D=on",
                ],
                "property_type" : "house"
            }

        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})
    

    def parse(self, response):
        for item in response.xpath("//article/div"):
            url = item.xpath(".//h3/a/@href").get()
            rented = item.xpath(".//span[contains(.,'Verhuurd')]/text()").get()
            if rented:
                continue
            print(url)
            yield Request(response.urljoin(url), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))

        item_loader.add_value("external_source", self.external_source)

        address =", ".join(response.xpath("//h1/text() | //h1/following-sibling::span[1]/text()").getall())
        if address:
            item_loader.add_value("address", address.strip())
        city = response.xpath("//h1/following-sibling::span[1]/text()").get()
        if city:    
            zipcode = " ".join(city.strip().split(" ")[:2])
            city = " ".join(city.strip().split(" ")[2:])
            item_loader.add_value("zipcode",zipcode) 
            item_loader.add_value("city",city) 

        title = response.xpath("//h1/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
        floor = response.xpath("//div[div[contains(.,'woonlagen')]]/div[2]/text()[normalize-space()]").get()
        if floor:
            item_loader.add_value("floor", floor.strip())
     
        square_meters = response.xpath("//div[div[contains(.,'Wonen')]]/div[2]/text()[normalize-space()]").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split('m')[0].split(',')[0].split('.')[0].strip())

        room_count = response.xpath("//div[div[.='Aantal kamers:']]/div[2]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split("slaap")[0].split("(")[-1].strip())
        
        bathroom_count = response.xpath("//div[div[.='Aantal badkamers:']]/div[2]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())

        rent = response.xpath("//div[div[contains(.,'Prijs')]]/div[2]/text()[normalize-space()]").get()
        if rent:
            item_loader.add_value("rent_string", rent)

        available_date = response.xpath("//div[div[contains(.,'Aangeboden sinds')]]/div[2]/text()[normalize-space()]").get()
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"], languages=['nl'])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
     
        images = [response.urljoin(x) for x in response.xpath("//div[@class='grid gap-4 md:grid-cols-2']//a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
    
        # try:
        #     latitude = response.xpath("//input[contains(@id,'mgmMarker')]/@value").get()
        #     if latitude:
        #         item_loader.add_value("latitude", latitude.split('~')[2].split(',')[0].strip())
        #         item_loader.add_value("longitude", latitude.split('~')[2].split(',')[1].strip())
        # except:
        #     pass
     

        item_loader.add_xpath("landlord_name", "//div[@class='xl:px-6 py-8']/h3/text()")
        item_loader.add_xpath("landlord_phone", "//div[@class='xl:px-6 py-8']/a[contains(@href,'tel')]/text()[normalize-space()]")
        
        yield item_loader.load_item()
