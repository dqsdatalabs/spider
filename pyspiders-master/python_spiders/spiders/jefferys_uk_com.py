# Author: Abbad49606a
import datetime
from ..loaders import ListingLoader
import re
from python_spiders.helper import format_date
import scrapy
import json


class JefferysSpider(scrapy.Spider):
    name = 'jefferys_uk_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'

    base_url = "https://www.jefferys.uk.com/properties-to-let?start=0"


    def start_requests(self):
        yield scrapy.Request(self.base_url, callback=self.parse, meta={"current_page": 0})

    def parse(self, response):
        current_page = response.meta.get("current_page")
        for listing in response.css('.eapow-overview-row'):
            yield scrapy.Request(response.urljoin(listing.css('.eapow-property-header-accent::attr(href)').get()), callback=self.get_info)

        current_page += 1
        if response.css(".pagination-list a::attr(title)").extract():
            if current_page < max([int(i) for i in response.css(".pagination-list a::attr(title)").extract()  if  i.isdigit()]):
                yield scrapy.Request(re.sub(r"\d+$", str(current_page * 10), self.base_url), callback=self.parse, meta={"current_page": current_page})

    def get_info(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", re.search(r"[\w-]+$", response.url).group().split("-")[0])
        item_loader.add_value("external_source", f"{''.join(self.name.split('_'))}_PySpider_{self.country}_{self.locale}")
        item_loader.add_value("title", response.css("title::text").get())
        description = response.css('h3:contains("Description")+div+div>p::text').get()
        if not description:
            description = response.css('h3:contains("DESCRIPTION")+div+div>p::text').get()
        if not description:
            description = "\n".join(response.css("#propDetails>div")[0].css("h3::text, p::text").extract())
        item_loader.add_value("description", description.strip())
        coordinates = response.css('script:contains("eapowmapoptions =")::text').get()
        if coordinates:
            coordinates = json.loads(coordinates.split("eapowmapoptions = ")[1].split(";")[0].replace("\r\n        ", '"').replace(":", '":'))
            item_loader.add_value("latitude", coordinates["lat"])
            item_loader.add_value("longitude", coordinates["lon"])
        address = response.css("address::text").get()
        if address:
            item_loader.add_value("zipcode", " ".join(address.split()[1:]))
        address = response.css('h1[class="span8 pull-left"]::text').get()
        if address:
            item_loader.add_value("address", address.strip())
        item_loader.add_value("property_type", "house")
        beds = response.css(".flaticon-bed+strong::text").get()
        if beds:
            item_loader.add_value("room_count", int(re.sub(r"[^\d]", "", beds)))
        baths = response.css(".flaticon-bath+strong::text").get()
        if baths:
            item_loader.add_value("bathroom_count", int(re.sub(r"[^\d]", "", baths)))
        temp_availability = response.css('h3:contains("Availability")+div+div>p::text').get()
        if temp_availability and "now" in temp_availability:
            item_loader.add_value("available_date", datetime.datetime.now().strftime('%Y-%m-%d'))
        temp = response.css('h3:contains("Tenure")+div+div>p::text').get()
        if temp:
            temp = temp.lower()
            if "unfurnished" in temp:
                item_loader.add_value("furnished", False)
            elif "furnished" in temp:
                item_loader.add_value("furnished", True)
        temp_deposit = response.css('h3:contains("Deposit")+div+div>p::text').get()
        deposit = temp_deposit if temp_deposit else response.css('h3:contains("DEPOSIT")+div+div>p::text').get()
        if deposit:
            item_loader.add_value("deposit", int(re.sub(r"[^\d]", "", deposit)))
        temp_pets = response.css('h3:contains("Restrictions")+div+div>p::text').get()
        if temp_pets:
            temp_pets = temp_pets.lower()
            if "no pets" in temp_pets:
                item_loader.add_value("pets_allowed", False)
            elif "pets" in temp_pets:
                item_loader.add_value("pets_allowed", True)
        images = response.css('.slides')[0]
        if images:
            images = images.css("img::attr(src)").extract()
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))
        item_loader.add_value("rent", int(re.sub(r"[^\d]", "", response.css(".eapow-detail-price::text").get())))
        item_loader.add_value("currency", "GBP")
        landlord_name = response.css(".span8>a>b::text").get()
        landlord_name = landlord_name if landlord_name else self.name
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_phone", "01726 73483")
        item_loader.add_value("landlord_email", "staustell@jefferys.uk.com")
        yield item_loader.load_item()