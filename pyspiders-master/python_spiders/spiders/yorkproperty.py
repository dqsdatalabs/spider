import scrapy
from python_spiders.helper import remove_white_spaces, sq_feet_to_meters
from python_spiders.loaders import ListingLoader
import dateutil.parser


class YorkpropertySpider(scrapy.Spider):
    name = 'yorkproperty'
    execution_type = 'testing'
    country = 'canada'
    locale = 'en'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['yorkproperty.ca']
    start_urls = ['https://www.yorkproperty.ca/apartments-for-rent/']

    def parse(self, response):
        for page in response.css(".wrap.more-details a::attr(href)").getall():
            yield scrapy.Request(url=response.urljoin(page), callback=self.parse_page)
    
    
    def parse_page(self, response):
        aparts = ['apartment']
        houses = ['house']
        
        
        images = response.css(".gallery-image .image img::attr(src)").getall()
        landlord_name = response.css(".agent-name::text").get()
        landlord_phone = response.css(".agent-number .hidden-mobile::text").get()
        title       = response.css("h1.property-title::text").get().strip()
        address, city= "".join(response.css("h4.property-address::text").getall()).strip().split(",")[0],"".join(response.css("h4.property-address::text").getall()).strip().split(",")[1]
        description     = remove_white_spaces(" ".join(response.css(".cms-content p::text").getall()))
        balcony,parking,dishwasher,elevator, washing_machine = fetch_amenities(response.css(".widget.amenities ul div div li::text").getall())
        pets_allowed = response.css(".pet-policy ul li::text").get()
        if pets_allowed:
            pets_allowed = pets_allowed.strip().lower() == "pet friendly"
        i = 1
        for apartment in response.css("tbody .no-description"):
            available_date = apartment.css(".list-availability a::text").get()
            if available_date:
                continue
            else:
                
                    available_date = apartment.css(".list-availability::text").get().strip().lower() 

                    if available_date == 'available':
                        available_date = ''
                    else:
                        available_date = apartment.css(".list-availability .hidden-mobile::text").get()
                        available_date = dateutil.parser.parse(available_date).strftime("%Y-%m-%d")
                        
                
                    property_type   = response.css("div.hide .location.property::attr(data-type)").get()
                    room_count      = apartment.css(".suite-beds span::text").getall()[1]
                    bathroom_count  = int(float(apartment.css(".suite-bath span::text").getall()[1]))
                    square_meters   = apartment.css(".suite-sq-ft span .value::text").get()
                    rent            = int(apartment.css(".suite-rate .value::text").get().replace("$","").replace(",",""))
                    deposit         = int(apartment.css(".suite-deposit div::text").get().strip().replace("$","").replace(",",""))
                    longitude       = response.css("div.hide .location.property::attr(data-longitude)").get()
                    latitude        = response.css("div.hide .location.property::attr(data-latitude)").get()
                    zipcode         = response.css("div.hide .location.property::attr(data-postal)").get()
                    external_id     = response.css("div.hide .location.property::attr(data-id)").get()

                    if room_count.lower() == "bachelor":
                        room_count = 1
                    else:
                        room_count = int(room_count)
                    if square_meters:
                        square_meters = sq_feet_to_meters(int(square_meters.strip()))

                    if property_type:
                        flag = True
                        for apart in aparts:
                            if apart in property_type.lower():
                                flag=False
                                property_type = 'apartment'
                        for house in houses:
                            if house in property_type.lower() and flag:
                                property_type = 'house'
                        if flag:
                            for apart in aparts:
                                if apart in title.lower():
                                    flag=False
                                    property_type = 'apartment'
                        for house in houses:
                            if house in title.lower() and flag:
                                property_type = 'house'
                            
                        

                    item = ListingLoader(response=response)
                    item.add_value("external_source"        ,self.external_source)
                    item.add_value("address"                ,address)
                    item.add_value("external_link"          ,response.url+"#{}".format(i))
                    item.add_value("currency"               ,"CAD")
                    item.add_value("title"                  ,title)
                    item.add_value("rent"                   ,rent)
                    item.add_value("images"                 ,images)
                    item.add_value("square_meters"          ,int(int(square_meters)*10.764))
                    item.add_value("room_count"             ,room_count)
                    item.add_value("bathroom_count"         ,bathroom_count)
                    item.add_value("property_type"          ,property_type)
                    item.add_value("parking"                ,parking)
                    item.add_value("available_date"         ,available_date)
                    item.add_value("description"            ,description)
                    item.add_value("landlord_phone"         ,landlord_phone)
                    item.add_value("landlord_name"          ,landlord_name)
                    item.add_value("deposit"                ,deposit)
                    item.add_value("balcony"                ,balcony)
                    item.add_value("dishwasher"             ,dishwasher)
                    item.add_value("elevator"               ,elevator)
                    item.add_value("washing_machine"        ,washing_machine)
                    item.add_value("pets_allowed"           ,pets_allowed)
                    item.add_value("longitude"              ,longitude)
                    item.add_value("latitude"               ,latitude)
                    item.add_value("zipcode"                ,zipcode)
                    item.add_value("external_id"            ,external_id)
                    item.add_value("city"                   ,city)
                    i+=1
                    yield item.load_item()

def fetch_amenities(vals):
    balcony,parking,dishwasher,elevator, washing_machine = '','','','',''
    for  val in vals:  
        if 'balcon' in val.lower():
            balcony = True
            
        if 'parking' in val.lower():
            parking = True
        
        if 'dishwasher' in val.lower():
            dishwasher = True
        
        if 'elevator' in val.lower():
            elevator = True
        
        if 'laundr' in val.lower() or 'washer' in val.lower():
            washing_machine = True
    
    return balcony,parking,dishwasher,elevator, washing_machine
