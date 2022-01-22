# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
import re


class MySpider(Spider):
    name = 'amsterdamhousing_com'
    start_urls = ['https://www.amsterdamhousing.com/en/realtime-listings/consumer?mode=recent']
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    external_source='Amsterdamhousing_PySpider_netherlands_nl'
    # 1. FOLLOWING
    def parse(self, response):
        data = json.loads(response.body)
        for item in data:
            if item["isRentals"]:
                url = response.urljoin(item["url"])      
                lat = item["lat"]
                lng = item["lng"]
                address =  item["address"]
                zipcode =  item["zipcode"]
                property_type = item["mainType"]
                if property_type not in ["apartment","house","room","studio"]:
                    propperty_type = None
                yield Request(url, callback=self.populate_item,meta={"lat":lat,"lng":lng,"address":address,"zipcode":zipcode,"property_type":property_type})

    # 2. SCRAPING level 2
    def populate_item(self, response):

        item_loader = ListingLoader(response=response)

        rented = response.xpath("//span[@class='details-status']/text()[contains(.,'Rented')]").get()
        if not rented:
            item_loader.add_value("external_source", "Amsterdamhousing_PySpider_" + self.country + "_" + self.locale)
            
            external_id = response.url
            item_loader.add_value("external_id", external_id.split("/")[-1])
            lat = response.meta.get("lat")
            lng = response.meta.get("lng")
            address = response.meta.get("address")
            zipcode = response.meta.get("zipcode")

            title = response.xpath("//h1/text()").get()
            if title:
                title = re.sub('\s{2,}', ' ', title.strip())
                item_loader.add_value("title", title)

            item_loader.add_value("external_link", response.url)

            price = response.xpath("//dl[@class='details-simple']/dt[.='Price']/following-sibling::dd[1]/text()[contains(.,'€')]").extract_first()
            if price:
                item_loader.add_value("rent", price.split("€")[1].split("p")[0])
            item_loader.add_value("currency", "EUR")

            property_type = response.meta.get('property_type')
            if  property_type== "other":
                item_loader.add_value("property_type", "house")
            else:
                item_loader.add_value("property_type", property_type)

            floor = "".join(response.xpath("substring-before(//p[@class='object-description']/text()[contains(.,'floor') and contains(.,'-') and not(contains(.,'under'))],'floor')").extract())
            if  floor:
                floor = floor.strip().replace("•","").replace("-","").replace("(","").split(" ")[-1].strip()
                if "tile" not in floor:
                    item_loader.add_value("floor", floor)
                
            square = response.xpath("//dl[@class='details-simple']/dt[.='Square Meter']/following-sibling::dd[1]/text()").get()
            if square:
                item_loader.add_value("square_meters", square.split("m²")[0])
            
            bathroom = response.xpath("//p[@class='object-description']/text()[contains(.,'Number of bathroom')]").get()
            if bathroom:
                item_loader.add_value("bathroom_count", bathroom.split(":")[1].strip().split(' ')[0].strip())

            images = [response.urljoin(x)for x in response.xpath("//div[@class='responsive-slider-slides']/div/img/@src").extract()]
            if images:
                    item_loader.add_value("images", images)
                
            item_loader.add_xpath("room_count","//dl[@class='details-simple']/dt[.='Rooms']/following-sibling::dd[1]/text()")
            
            available_date = response.xpath("//dl[@class='details-simple']/dt[.='Available from']/following-sibling::dd[1]/text()[. !='directly' and . !='in consultation' ]").extract_first()
            if available_date:
                date_parsed = dateparser.parse(available_date, date_formats=["%d %B %Y"])
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)

            desc = "".join(response.xpath("//p[@class='object-description']/text()").extract())
            if desc:
                desc = re.sub('\s{2,}', ' ', desc.strip())
                item_loader.add_value("description", desc)

            pets_allowed = response.xpath("//text()[contains(.,'Pets allowed')]").get()
            if pets_allowed:
                item_loader.add_value("pets_allowed", True)

            utilities = response.xpath("//text()[contains(.,'advance payment')]").get()
            if utilities:
                item_loader.add_value("utilities", "".join(filter(str.isnumeric, utilities.strip())))

           
            if "washing machine" in desc or "Washing machine" in desc or "Washer" in desc or "washer" in desc:
                item_loader.add_value("washing_machine", True)
            
            if "pets negotiable" in desc or "Pets negotiable" in desc:
                item_loader.add_value("pets_allowed", True)
            
            if "swimming pool" in desc or "pool" in desc:
                item_loader.add_value("swimming_pool", True)

            if "dishwasher" in desc or "Dishwasher" in desc:
                item_loader.add_value("dishwasher", True)

            if "Balcony" in desc or "balcony" in desc:
                item_loader.add_value("balcony", True)

            if "Elevator" in desc or "elevator" in desc:
                item_loader.add_value("elevator", True)

            furnished = response.xpath("//dl[@class='details-simple']/dt[.='Decoration']/following-sibling::dd[1]/text()").get()
            if not furnished:
                furnished = response.xpath("//p[@class='object-description']/text()[contains(.,'Interior decoration:')]").get()
            if furnished:
                if "unfurnished" in furnished.lower():
                    item_loader.add_value("furnished", False)
                elif "furnished" in furnished.lower():
                    item_loader.add_value("furnished", True)


            terrace = response.xpath("normalize-space(//p[@class='object-description']/text()[contains(.,'Parking possibility:') or contains(.,'Private parking')] | //tr[td[.='Soort garage']]/td[2]/text() | //tr[td[.='Soort parkeergelegenheid']]/td[2]/text())").get()
            if terrace:
                item_loader.add_value("parking", True)
            else:
                parking = response.xpath("//dt[contains(.,'Parking')]/following-sibling::dd[1]/text()[contains(.,'Indoor') or contains(.,'Garage')]").get()
                if parking:
                    item_loader.add_value("parking", True)

            
            city = "".join(response.xpath("//dl[@class='details-simple']/dt[.='City']/following-sibling::dd[1]/text()").extract())
            if city:
                item_loader.add_value("city", re.sub("\s{2,}", " ", city.strip()))

            item_loader.add_value("latitude", str(lat))
            item_loader.add_value("longitude", str(lng))
            item_loader.add_value("zipcode", str(zipcode).split(" ")[0])
            item_loader.add_value("address", str(address))

            item_loader.add_xpath("landlord_phone", "//div[contains(@class,'text-center')]/p/big/a/text()")
            item_loader.add_xpath("landlord_email", "//div[@class='row']/div/p/a/text()[contains(.,'info')]")
            item_loader.add_value("landlord_name", "Aamsterdam Housing")

            yield item_loader.load_item()