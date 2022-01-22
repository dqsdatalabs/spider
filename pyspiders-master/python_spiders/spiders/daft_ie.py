# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
class MySpider(Spider):
    name = 'daft_ie_disabled'
    execution_type='testing'
    country='ireland'
    locale='en'
 
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.daft.ie/property-for-rent/ireland/apartments",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.daft.ie/property-for-rent/ireland/houses",
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://www.daft.ie/property-for-rent/ireland/studio-apartments",
                ],
                "property_type" : "studio",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//ul[@data-testid='results']/li"):
            subitems = item.xpath("./div//li/a/@href").getall()
            if len(subitems) > 0:
                for subitem in subitems: yield Request(response.urljoin(subitem), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
            else:
                follow_url = response.urljoin(item.xpath("./a/@href").get())
                yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        next_button = response.xpath("//a[contains(.,'Next page')]/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse, meta={"property_type":response.meta["property_type"]})
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("/")[-1].strip())

        if '?redirectReason=expired' in response.url: return

        item_loader.add_value("external_source", "Daft_PySpider_ireland")
        title = "".join(response.xpath("//h1[@data-testid='address']//text()").getall())
        if title:
            item_loader.add_value("title", title.strip())
        else:
            title = "".join(response.xpath("//h1/text()").getall())
            if title:
                item_loader.add_value("title", title.strip())
        address = "".join(response.xpath("//h1[@data-testid='address']//text()").getall())
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.split(",")[-1].replace("Co.","").strip())
        else:
            address = response.xpath("//h1/text()").get()
            if address:
                item_loader.add_value("address", address.strip())
                city = address.split(",")[-1].replace("Co.","").strip()
                if "Student Campus" not in city:
                    item_loader.add_value("city",city)
                elif "Student Campus" in city:
                    city = address.split(",")[1].split(",")[0].strip()
                    if city:
                        item_loader.add_value("city", city)
        room_count = response.xpath("//div[@data-testid='card-info']/p[contains(.,'Bed')]/text()").get()
        prop_type=""
        if room_count:                   
            item_loader.add_value("room_count",room_count.split("Bed")[0].strip())
        elif "studio" in response.meta.get('property_type'):
            item_loader.add_value("room_count", "1")
        else:
            room = response.xpath("//div[@data-testid='card-info']/p[contains(.,'Studio')]/text()").get()
            if room and "studio" in room.lower():
                prop_type = "studio"
                item_loader.add_value("room_count", "1")
        if prop_type:
            item_loader.add_value("property_type", prop_type)
        else:
            item_loader.add_value("property_type", response.meta.get('property_type'))
            
        bathroom_count = response.xpath("//div[@data-testid='card-info']/p[contains(.,'Bath')]/text()").get()
        if bathroom_count:                   
            item_loader.add_value("bathroom_count",bathroom_count.split("Bath")[0].strip())    

        available_date = "".join(response.xpath("//li[span[.='Available From']]/text()").getall())
        if available_date:
            date_parsed = dateparser.parse(available_date.replace(":","").replace("Immediately","now").strip(), date_formats=["%d %m %Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
    
        description = " ".join(response.xpath("//div[@data-testid='description']/text()").getall())  
        if description:
            item_loader.add_value("description", description.strip())       
        
        energy_label = response.xpath("//div[@data-testid='ber']/img/@alt[.!='SI_666']").get()
        if energy_label and energy_label !='NA':                   
            item_loader.add_value("energy_label",energy_label.strip())
        furnished = "".join(response.xpath("//li[span[.='Furnished']]/text()").getall())
        if furnished:
            if "no" in furnished.lower():
                item_loader.add_value("furnished",False)
            elif "yes" in furnished.lower():
                item_loader.add_value("furnished",True)
        parking = response.xpath("//div[@data-testid='facilities']//div[@data-testid='read-more-contents']//li[.='Parking']/text()").get()
        if parking:                   
            item_loader.add_value("parking", True)
        else:
            parking = response.xpath("//div[@data-testid='facilities']/ul/li[.='Parking']/text()").get()
            if parking:                   
                item_loader.add_value("parking", True)           
        balcony = response.xpath("//div[@data-testid='facilities']//div[@data-testid='read-more-contents']//li[contains(.,'Balcony')]/text()").get()
        if balcony:                   
            item_loader.add_value("balcony", True)
        washing_machine = response.xpath("//div[@data-testid='facilities']//div[@data-testid='read-more-contents']//li[.='Washing Machine']/text()").get()
        if washing_machine:                   
            item_loader.add_value("washing_machine", True)
        dishwasher = response.xpath("//div[@data-testid='facilities']//div[@data-testid='read-more-contents']//li[.='Dishwasher']/text()").get()
        if dishwasher:                   
            item_loader.add_value("dishwasher", True)
        json_data = response.xpath("//script[@id='__NEXT_DATA__']//text()").get()  
        data = json.loads(json_data)
        item = data["props"]["pageProps"]["listing"]
        rent = item["price"]
        lat_lng = item["point"]["coordinates"]
        if lat_lng:
            lat = lat_lng[0]
            lng = lat_lng[1]
            item_loader.add_xpath("latitude", str(lat)) 
            item_loader.add_xpath("longitude", str(lng)) 

        rent = response.xpath("//div[@data-testid='price']/p/span/text()").extract_first()
        if rent:
            price = rent.replace(",","").strip().split(" ")[0]
            item_loader.add_value("rent", price)

        # if rent:
            
        #     if "per week" in rent:
        #         rent = rent.split('€')[-1].split('per')[0].strip().replace(',', '').replace('\xa0', '').replace("From","").replace(")","").strip()
        #         item_loader.add_value("rent", int(rent) * 4)
        #     else:    
        #         if rent.isdigit():
        #             rent = rent.split('€')[-1].split('per')[0].strip().split(")")[0].replace(',', '').replace('\xa0', '')
        #             item_loader.add_value("rent", str(int(float(rent))))

        item_loader.add_value("currency", 'EUR')

        if "images" in item["media"]:
            images = [response.urljoin(x["size1440x960"]) for x in item["media"]["images"]]
            if images:
                item_loader.add_value("images", images)
        item_loader.add_value("landlord_name", item["seller"]["name"])
        if "phone" in item["seller"]:
            item_loader.add_value("landlord_phone", item["seller"]["phone"])
 
        yield item_loader.load_item()