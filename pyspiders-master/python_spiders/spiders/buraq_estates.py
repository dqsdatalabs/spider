# Author: Abbad49606a
import datetime
from ..loaders import ListingLoader 
import re
import json
from python_spiders.helper import format_date
import scrapy 
import datetime 
 

class BuraqEstatesSpider(scrapy.Spider):
    name = 'buraq_estates' 
    execution_type='testing' 
    country='united_kingdom' 
    locale='en'
    external_source='buraqestates_PySpider_united_kingdom_en'
    start_urls = [
        "https://www.buraqestates.co.uk/student-lettings?view=grid&page=1",
        "https://www.buraqestates.co.uk/residential-lettings?view=grid&page=1"
    ] 
 

    def start_requests(self): 
        for i in self.start_urls:
            yield scrapy.Request(i, callback=self.parse, meta={"current_page": 1})

    def parse(self, response):
        current_page = response.meta.get("current_page")

        for listing in response.xpath("//div[@class='card']"):
            if listing.xpath("//div[@class='card-label']/text()").get() == "Let Agreed":
                continue
            yield scrapy.Request(listing.xpath('./a/@href').get(), callback=self.get_info)

        # if current_page <= max([int(i) for i in response.css(".pagination a::attr(data-page)").extract()]):
        #     current_page += 1
        #     yield scrapy.Request(re.sub(r"\d+$", str(current_page), response.url), callback=self.parse, meta={"current_page": current_page})
 
    def get_info(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        # item_loader.add_value("external_id", re.search(r"[\w-]+$", response.url).group())
        external_id=re.search(r"[\w-]+$", response.url).group()
        external_id=re.findall("\d+",external_id)
        item_loader.add_value("external_id", external_id)

        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("title", response.css("title::text").get())
        description = "\n".join([i.strip() for i in response.css(".property-content *::text").extract() if i.strip()])
        if not description:
            description = "\n".join([i.strip() for i in response.css("#tab-details>p>span::text").extract() if i.strip()])
        if not description:
            description = "\n".join([i.strip() for i in response.css("#tab-details>p::text").extract() if i.strip()])
        item_loader.add_value("description", description.strip())
        address = response.css("h2::text").get()
        if address.split(",")[-1]:
            item_loader.add_value("city", address.split(",")[-1].split()[0])

 

        zipcode =response.css("h2::text").get().split(",")[-1].strip().split()
        # if re.search(r"\d+", zipcode):
        #     item_loader.add_value("zipcode", zipcode) 
        r=""
        for i in zipcode:
            if re.match(".[0-9A-Z]",i):
                p=re.compile('.*[0-9A-Z]')
                t=p.match(i).span()
                k=str(t).split("(")[1].split(",")[0]
                r+=i[int(k):]
        if len(r)>0:
           item_loader.add_value("zipcode", r)
 
        




        item_loader.add_value("address", address)
        coordinates = response.css(".js-json-map-results::text").get()
        if coordinates:
            coordinates = json.loads(coordinates)[0]
            item_loader.add_value("latitude", coordinates["latitude"])
            item_loader.add_value("longitude", coordinates["longitude"])
        if "student-lettings" in response.url:
            property_type = "student_apartment"
        if response.css("h1::text").get().split()[-1].strip().lower() in ["apartment", "flat"] or "apartment" in description.lower():
            property_type = "apartment"
        elif "house" in response.css("h1::text").get().lower() or "house" in description.lower():
            property_type = "house"
        item_loader.add_value("property_type", property_type)
        available = "".join(response.css(".property-available::text").extract()).strip().lower()
        if "available" in available:
            item_loader.add_value('available_date', datetime.datetime.strftime(datetime.datetime.strptime(available.split(": ")[-1] + " 2021", "%d %B %Y"), '%Y-%m-%d'))
        item_loader.add_value("room_count", int(re.sub(r"[^\d]", "", response.css("h1::text").get())))
        for feature in response.css('#tab-details li::text').extract():
            feature = feature.strip().lower()
            if "bathroom" in feature:
                bathroom = re.sub(r"[^\d]", "", feature)
                if bathroom.isdigit():
                    item_loader.add_value("bathroom_count", int(bathroom))
            elif "parking" in feature:
                item_loader.add_value("parking", True)
            feature = [" "] + feature.split()
            if feature[-1] == "furnished":
                item_loader.add_value("furnished", True)
        images = response.css('.carousel-slide a::attr(href)').extract()
        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count", len(images))
        rent = int(re.sub(r"[^\d]", "", [i.strip() for i in response.css(".property-price::text").extract() if i.strip()][0]))
        if "Week" in response.css(".property-price>small::text").get():
            rent = rent * 4
            
        item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")
        item_loader.add_value("landlord_name", self.name)
        item_loader.add_value("landlord_phone", "0161 248 4585")
        item_loader.add_value("landlord_email", "info@buraqestates.co.uk")
        yield item_loader.load_item()