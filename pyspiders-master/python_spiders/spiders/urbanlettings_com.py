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
    name = 'urbanlettings_com'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    external_source = 'Urbanlettings_PySpider_united_kingdom'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.zoopla.co.uk/to-rent/branch/urban-lettings-london-19863/?branch_id=19863&include_rented=true&include_shared_accommodation=false&price_frequency=per_month&property_type=flats&results_sort=newest_listings&search_source=refine",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.zoopla.co.uk/to-rent/branch/urban-lettings-london-19863/?branch_id=19863&include_rented=true&include_shared_accommodation=false&price_frequency=per_month&property_type=houses&results_sort=newest_listings&search_source=refine",
                    
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='status-wrapper']"):
            status = item.xpath(".//span[contains(@class,'status-text')]/text()").get()
            if status and "agreed" in status.lower():
                continue
            follow_url = response.urljoin(item.xpath("./a[not(@class)]/@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

        next_page = response.xpath("//a[.='Next']/@href").get()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse,
                meta={"property_type":response.meta["property_type"]})    
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("details/")[1].split("/")[0])

        rented = "".join(response.xpath("//li[@class='ui-property-indicators__item']/span/text()").extract())
        if "let" in rented.lower():
            return

        if "Studio" in response.xpath("//span[contains(@data-testid,'title')]/text()").get():
            item_loader.add_value("property_type", "studio")
            item_loader.add_value("room_count", "1")
        else:
            item_loader.add_value("property_type", response.meta.get('property_type'))

        item_loader.add_xpath("title", "//span[contains(@data-testid,'title')]/text()")
        item_loader.add_value("external_source", self.external_source)
        dontallow=item_loader.get_output_value("title")
        if dontallow and "garage" in dontallow:
            return 

        rent = response.xpath("//span[contains(@data-testid,'price')]//text()").extract_first()
        if rent:
            item_loader.add_value("rent", rent.split("pcm")[0].replace(",","").replace("£",""))
            item_loader.add_value("currency", "GBP")

        room = response.xpath("//span[contains(@data-testid,'beds')]//text()").extract_first()
        if room:
            item_loader.add_value("room_count", room.split(" ")[0])

        bathroom_count = response.xpath("//span[contains(@data-testid,'bath')]//text()").extract_first()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split(" ")[0])

        address = " ".join(response.xpath("//span[contains(@data-testid,'address')]//text()").extract())
        if address:           
            zipcode = address.split(" ")[-1]
            city = address.split(zipcode)[0].split(",")[-1]
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", city.strip())
            item_loader.add_value("zipcode", zipcode.strip())

        square_meters = response.xpath("//span[contains(@data-testid,'floorarea-label')]//text()").get()
        if square_meters:
            multiple = 0.09290304 if 'ft' in square_meters else 1
            square_meters = "".join(filter(str.isnumeric, square_meters))
            if square_meters.isnumeric(): item_loader.add_value("square_meters", str(int(float(square_meters) * multiple)))

        from datetime import datetime
        from datetime import date
        import dateparser
        available_date = response.xpath("//span[contains(@data-testid,'availability')]//text()").get()
        if available_date:
            if not "immediately" in available_date.lower():
                available_date = available_date.split("from")[1].strip()
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

        desc = " ".join(response.xpath("//div[contains(@data-testid,'listing_description')]//text()").getall()).strip()
        if desc:
            item_loader.add_value("description", desc)

        # label = "".join(response.xpath("//div[@class='dp-description__text']/text()[contains(.,'EPC')]").getall())
        # if label:
        #     item_loader.add_value("energy_label", label.split("Band")[1].replace(".",""))

        latitude_longitude = response.xpath("//script[contains(.,'GeoCoordinates')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('latitude":')[1].split(',')[0]
            longitude = latitude_longitude.split('longitude":')[1].split('}')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        # water_cost = "".join(response.xpath("substring-before(substring-after(//a[span[contains(.,'Water')]]/span[2]/text(),'from'),'p/m')").getall())
        # if water_cost:
        #     item_loader.add_value("water_cost", water_cost.strip())
        
        script_image = response.xpath("//script[contains(@id,'__NEXT_DATA__')]//text()").get()
        if script_image:
            image_url = "https://lid.zoocdn.com/u/2400/1800/"  
            images = json.loads(script_image)
            image = images["props"]["pageProps"]["data"]["listing"]["propertyImage"]
            images = []
            for x in image:
                x =f"https://lid.zoocdn.com/u/2400/1800/{x['filename']}"
                images.append(x)
                item_loader.add_value("images", images)

        furnished = " ".join(response.xpath("//div[contains(@data-testid,'listing_features')]//li[contains(.,'Furnished')]//text()").extract())
        if furnished:
            item_loader.add_value("furnished", True) 

        terrace = " ".join(response.xpath("//div[contains(@data-testid,'listing_features')]//li[contains(.,'Terrace')]//text()").extract())
        if terrace:
            item_loader.add_value("terrace", True) 

        parking = " ".join(response.xpath("//div[contains(@data-testid,'listing_features')]//li[contains(.,'parking') or contains(.,'Parking') or contains(.,'garage')]/text()").extract())
        if parking:
            item_loader.add_value("parking", True) 
        
        balcony = " ".join(response.xpath("//div[contains(@data-testid,'listing_features')]//li[contains(.,'Balcon')]/text()").extract())
        if balcony:
            item_loader.add_value("balcony", True) 

        item_loader.add_value("landlord_name", "Urban Lettings")
        item_loader.add_value("landlord_phone", "020 3463 9320")
       
        yield item_loader.load_item()