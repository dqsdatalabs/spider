# -*- coding: utf-8 -*-
# Author: Fill with the developer's Name
import scrapy
from ..loaders import ListingLoader
from ..helper import *
class HighstreetlivingCaSpider(scrapy.Spider):
    name = "highstreetliving_ca"
    start_urls = ['https://highstreetliving.ca/locations/']
    allowed_domains = ["highstreetliving.ca"]
    country = 'canada' # Fill in the Country's name
    locale = 'en' # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'
    position = 1
    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)
    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        for url in response.css('.box-link::attr(href)').extract():
            yield scrapy.Request(url, callback=self.populate_item)

    def get_description(self, all_possible):
        mx = 1
        desc = ''
        valid = [i if i is not None else '' for i in all_possible]

        for i in valid:
            if len(i) > mx:
                desc = i
                mx = len(i)

        return desc

    # 3. SCRAPING level 3
    def populate_item(self, response):
        title = response.css('h1::text').get()
        description = response.css('b::text').get()
        description = description_cleaner(description)
        last = ''
        t = response.xpath('//*[@id="fws_61df1b3c47a22"]/div[3]/div/div/div[2]/div[4]/div/p/span/b[1]/text()').get()
        f= response.xpath('//*[@id="fws_61df1b3c47a22"]/div[3]/div/div/div[2]/div[4]/div/p/span/b[2]/text()').get()
        if t is not None:
            last+= t

        if f is not None:
            last+=f

        a = [response.css('p span b strong::text').get(),response.css('p strong span::text').get() , response.css('b::text').get(),
             response.xpath('//*[@id="fws_61df1b3d53ffc"]/div[2]/div/div/div[2]/div/div/p/strong/span/text()').get(), last]
        description = self.get_description(a)

        #description = description_cleaner(description)
        details = response.css('.nectar-fancy-ul strong::text').getall()+[i.strip() for i in response.css('li::text').getall() if i.strip() !='']

        city = response.css('h6::text').get().split(',')[0].split()[-1].lower()
        pets_allowed = furnished = parking = elevator = balcony = terrace = swimming_pool = washing_machine = dishwasher = None
        pets_allowed, furnished, parking, elevator, balcony, terrace, swimming_pool, washing_machine, dishwasher = get_amenities(
            description, ' '.join(details))
        images = [i for i in response.css('img::attr(src)').extract() if '.jpg' in i]
        room_count = bathroom_count = 1
        names = response.css('h2::text').getall()
        prices = [i.split('$')[1].split(' –')[0] for i in response.css('li:nth-child(1) h5::text').getall()]
        rooms = [re.search(r'\d+', i)[0] for i in response.css('li:nth-child(2) h5::text').getall()]
        bathrooms = [re.search(r'\d+', i)[0] for i in response.css('li:nth-child(3) h5::text').getall()]
        sizes = [re.search(r'\d+', i)[0] for i in response.css('li:nth-child(4) h5::text').getall()]
        if '.jpg' in response.css('.column-image-bg').get():
            floor_plan = [re.search(r'https(.*).jpg', i)[0] for i in response.css('.column-image-bg').getall()]
        elif '.png' in response.css('.column-image-bg').get():
            floor_plan = [re.search(r'https(.*).png', i)[0] for i in response.css('.column-image-bg').getall()]
        for i in range(len(prices)):
            item_loader = ListingLoader(response=response)
            name = names[i]
            rent = int(float(re.sub(r"[_,.*+(){}';@#?!&$/-]+\ *",'',prices[i])))
            room_count = int(float(rooms[i]))
            bathroom_count = int(float(bathrooms[i]))
            square_feet = int(float(sizes[i]))
            floor_plan_images = floor_plan[i]
            # Enforces rent between 0 and 40,000 please don't delete these lines
            if 0 >= int(rent) > 40000:
                return
            # # MetaData
            item_loader.add_value("external_link", response.url+'#'+str(self.position)) # String
            item_loader.add_value("external_source", self.external_source) # String
            #item_loader.add_value("external_id", external_id) # String
            item_loader.add_value("position", self.position) # Int
            item_loader.add_value("title", title+''+name) # String
            item_loader.add_value("description", description) # String
            # # Property Details
            item_loader.add_value("city", city) # String
            #item_loader.add_value("zipcode", zipcode) # String
            item_loader.add_value("address", city) # String
            #item_loader.add_value("latitude", latitude) # String
            #item_loader.add_value("longitude", longitude) # String
            #item_loader.add_value("floor", floor) # String
            item_loader.add_value("property_type", 'apartment') # String => ["apartment", "house", "room", "student_apartment", "studio"]
            item_loader.add_value("square_meters", square_feet) # Int
            item_loader.add_value("room_count", room_count) # Int
            item_loader.add_value("bathroom_count", bathroom_count) # Int
            #item_loader.add_value("available_date", available_date) # String => date_format
            item_loader.add_value("pets_allowed", pets_allowed) # Boolean
            item_loader.add_value("furnished", furnished) # Boolean
            item_loader.add_value("parking", parking) # Boolean
            item_loader.add_value("elevator", elevator) # Boolean
            item_loader.add_value("balcony", balcony) # Boolean
            item_loader.add_value("terrace", terrace) # Boolean
            item_loader.add_value("swimming_pool", swimming_pool) # Boolean
            item_loader.add_value("washing_machine", washing_machine) # Boolean
            item_loader.add_value("dishwasher", dishwasher) # Boolean
            # # Images
            item_loader.add_value("images", images) # Array
            item_loader.add_value("external_images_count", len(images)) # Int
            item_loader.add_value("floor_plan_images", floor_plan_images) # Array
            # # Monetary Status
            item_loader.add_value("rent", rent) # Int
            #item_loader.add_value("deposit", deposit) # Int
            #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
            #item_loader.add_value("utilities", utilities) # Int
            item_loader.add_value("currency", "CAD") # String
            #item_loader.add_value("water_cost", water_cost) # Int
            #item_loader.add_value("heating_cost", heating_cost) # Int
            #item_loader.add_value("energy_label", energy_label) # String
            # # LandLord Details
            item_loader.add_value("landlord_name", 'Jeff Wilkins') # String
            item_loader.add_value("landlord_phone", '778.784.7838') # String
            item_loader.add_value("landlord_email", 'leasing@hsliving.ca') # String
            self.position += 1
            yield item_loader.load_item()