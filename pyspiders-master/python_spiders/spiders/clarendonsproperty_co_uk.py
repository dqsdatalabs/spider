from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re



class MySpider(Spider):
    name = 'clarendonsproperty_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    external_source = "Clarendonspropertycouk_PySpider_united_kingdom"
    page_num = 1
    custom_settings = {
       "HTTPCACHE_ENABLED": False,
    }

    def start_requests(self):
        yield Request("https://www.clarendonsproperty.co.uk/search/1.html?showstc=on&showsold=on&instruction_type=Letting", callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):
        total_page = response.xpath("//span[@class='props-number']/text()").get()

        for item in response.xpath("//div[@id='search-results']/div"):
            follow_url = "https://www.clarendonsproperty.co.uk" + item.xpath(".//div[@class='property']/div/a/@href").get()

            let_agreed = item.xpath(".//text()[.='AGREED']").get()
            if not bool(let_agreed):
                yield Request(follow_url, callback=self.populate_item)

            

        
        if self.page_num < (int(total_page)/10+1):
            self.page_num +=1
            url = f"https://www.clarendonsproperty.co.uk/search/{self.page_num}.html?showstc=on&showsold=on&instruction_type=Letting"
            yield Request(url, callback=self.parse)

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        property_type = response.xpath("//title//text()").get()
        if property_type and "maisonette" in property_type.lower():
            item_loader.add_value("property_type","House")
        elif property_type and "apartment" in property_type.lower():
            item_loader.add_value("property_type","Apartment")
        else:
            item_loader.add_value("property_type","House")

        title = response.xpath("//div/h1/text()").get()
        if title:
            item_loader.add_value("title",title)
            zipcode = title.split(",")[-1].strip()
            if zipcode != 'Surrey':
                item_loader.add_value("zipcode",zipcode)
            item_loader.add_value("address",title)

        rent = response.xpath("//div/h2/text()").get()
        if rent:
            rent = rent.split()[-2].replace("£","").replace(",","").strip()
            item_loader.add_value("rent",rent)

        images = []
        images = ["https://www.clarendonsproperty.co.uk/" + img for img in response.xpath("//div[@class='carousel-inner']/div/img/@src").getall()]
        if images:
            item_loader.add_value("images",images)
            item_loader.add_value("external_images_count",len(images))

        room_count = response.xpath("//*[@class='icon-bedrooms']/following-sibling::strong/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)

        bathroom_count = response.xpath("//*[@class='icon-bathrooms']/following-sibling::strong/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)
        bathroomcheck=item_loader.get_output_value("bathroom_count")
        if not bathroomcheck:
            bath=response.xpath("//li[contains(.,'Shower')]/text()").get()
            if bath:
                bath=bath.split("Shower")[0].strip()
                if bath and "three" in bath.lower(): 
                    item_loader.add_value("bathroom_count","3")
                if bath and "two" in bath.lower():
                    item_loader.add_value("bathroom_count","2")

        desc = " ".join(response.xpath("//div[@id='property-long-description']/div/div/text()").getall())
        if desc:
            item_loader.add_value("description",desc)

        position = response.xpath("//script[contains(text(),'latitude')]/text()").get()
        if position:
            lat = re.search('latitude": "([\d.-]+)',position).group(1)
            long = re.search('"longitude": "([\d.-]+)',position).group(1)
            
            item_loader.add_value("latitude",lat)
            item_loader.add_value("longitude",long)

        external_id = response.url.split("clarlettings-")[-1].split("/")[0]
        item_loader.add_value("external_id",external_id)

        if "furnished" in desc.lower():
            item_loader.add_value("furnished",True)
        if "apartment" in desc.lower():
            item_loader.add_value("property_type","apartment")
        if "house" in desc.lower():
            item_loader.add_value("property_type","house")
        if "parking" in desc.lower():
            item_loader.add_value("parking",True)

        
        item_loader.add_value("landlord_phone","01737 230 821")
        item_loader.add_value("landlord_email","enquiries@clarendonsproperty.co.uk")
        item_loader.add_value("landlord_name","Clarendons agency")
        item_loader.add_value("currency","GBP")
        item_loader.add_value("city","Surrey")
        item_loader.add_value("external_source",self.external_source)
        

        
        yield item_loader.load_item()

            